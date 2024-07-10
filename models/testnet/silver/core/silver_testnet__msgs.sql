{{ config(
  materialized = 'incremental',
  incremental_predicates = ['DBT_INTERNAL_DEST.partition_key >= (select min(partition_key) from ' ~ generate_tmp_view_name(this) ~ ')'],
  unique_key = ["tx_id","msg_index"],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['modified_timestamp::DATE','partition_key'],
  tags = ['core_testnet','full_test']
) }}
{# post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(msg_type, msg:attributes);", #}
WITH b AS (

  SELECT
    block_id,
    tx_id,
    tx_succeeded,
    INDEX AS msg_index,
    VALUE :type :: STRING AS msg_type,
    VALUE AS msg,
    IFF(
      msg :attributes [0] :key :: STRING = 'action',
      TRUE,
      FALSE
    ) AS is_action,
    IFF(
      msg :attributes [0] :key :: STRING = 'module',
      TRUE,
      FALSE
    ) AS is_module,
    msg :attributes [0] :key :: STRING AS attribute_key,
    msg :attributes [0] :value :: STRING AS attribute_value,
    partition_key
  FROM
    {{ ref('silver_testnet__transactions') }} A,
    LATERAL FLATTEN(
      input => A.msgs
    )

{% if is_incremental() %}
WHERE
  modified_timestamp >= DATEADD(
    'minute',
    -5,(
      SELECT
        MAX(
          modified_timestamp
        )
      FROM
        {{ this }}
    )
  ) {# block_id >= 713035 #}
{% endif %}
),
prefinal AS (
  SELECT
    block_id,
    tx_id,
    tx_succeeded,
    NULLIF(
      (conditional_true_event(is_action) over (PARTITION BY tx_id
      ORDER BY
        msg_index) -1),
        -1
    ) AS msg_group,
    msg_index,
    msg_type,
    msg,
    is_module,
    attribute_key,
    attribute_value,
    partition_key
  FROM
    b
),
grp AS (
  SELECT
    tx_id,
    msg_index,
    is_module,
    RANK() over(
      PARTITION BY tx_id,
      msg_group
      ORDER BY
        msg_index
    ) -1 AS msg_sub_group
  FROM
    prefinal
  WHERE
    is_module
    AND msg_type = 'message'
)
SELECT
  block_id,
  A.tx_id,
  tx_succeeded,
  msg_group,
  COALESCE(
    CASE
      WHEN msg_group IS NULL THEN NULL
      ELSE LAST_VALUE(
        b.msg_sub_group ignore nulls
      ) over(
        PARTITION BY A.tx_id,
        msg_group
        ORDER BY
          A.msg_index DESC rows unbounded preceding
      )
    END,
    CASE
      WHEN msg_group IS NOT NULL THEN 0
    END
  ) AS msg_sub_group,
  A.msg_index,
  msg_type,
  msg,
  {{ dbt_utils.generate_surrogate_key(
    ['a.tx_id','a.msg_index']
  ) }} AS msgs_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  partition_key,
  '{{ invocation_id }}' AS _invocation_id
FROM
  prefinal A
  LEFT JOIN grp b
  ON A.tx_id = b.tx_id
  AND A.msg_index = b.msg_index
