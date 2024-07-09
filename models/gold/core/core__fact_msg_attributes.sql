{{ config(
  materialized = 'incremental',
  incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
  unique_key = ["tx_id","msg_index","attribute_index"],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,msg_type,attribute_key,attribute_value);",
  tags = ['core','full_test']
) }}

SELECT
  block_id,
  block_timestamp,
  tx_id,
  tx_succeeded,
  msg_group,
  msg_sub_group,
  msg_index,
  msg_type,
  attribute_index,
  attribute_key,
  attribute_value,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_id','msg_index','attribute_index']
  ) }} AS fact_msg_attributes_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  {{ ref('silver__msg_attributes') }}

{% if is_incremental() %}
WHERE
  modified_timestamp >= DATEADD(
    'minute',
    -45,(
      SELECT
        MAX(
          modified_timestamp
        )
      FROM
        {{ this }}
    )
  )
{% endif %}
