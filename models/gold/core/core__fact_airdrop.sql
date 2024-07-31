{{ config(
  materialized = 'incremental',
  incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
  unique_key = ["tx_id","msg_index"],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['modified_timestamp::DATE'],
  tags = ['core','full_test']
) }}

SELECT
  block_id,
  block_timestamp,
  tx_id,
  tx_succeeded,
  msg_index,
  receiver AS recipient,
  amount / pow(
    10,
    7
  ),
  {{ dbt_utils.generate_surrogate_key(
    ['tx_id','msg_index']
  ) }} AS airdrop_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  {{ ref('silver__transfers') }}
WHERE
  block_timestamp :: DATE = '2024-07-30'
  AND sender = 'lava@188kzvhru5ch303a2h78a2kya9dp7gup9fkpd2t'
