{{ config(
  materialized = 'view',
  tags = ['core_testnet','full_test']
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
  {{ ref('core_testnet__fact_msgs') }} A,
  LATERAL FLATTEN(
    input => A.msg,
    path => 'attributes'
  ) b
