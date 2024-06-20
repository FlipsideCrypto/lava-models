{{ config(
  materialized = 'incremental',
  incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
  unique_key = ["tx_id","msg_index","attribute_index"],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
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
  b.index AS attribute_index,
  b.value :key :: STRING AS attribute_key,
  b.value :value :: STRING AS attribute_value,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_id','msg_index','attribute_index']
  ) }} AS msg_attributes_id,
  inserted_timestamp,
  modified_timestamp,
  _invocation_id
FROM
  {{ ref('core__fact_msgs') }} A,
  LATERAL FLATTEN(
    input => A.msg,
    path => 'attributes'
  ) b

{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(
        modified_timestamp
      )
    FROM
      {{ this }}
  )
{% endif %}
