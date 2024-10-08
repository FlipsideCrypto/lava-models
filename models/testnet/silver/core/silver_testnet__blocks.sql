{{ config(
    materialized = 'incremental',
    unique_key = "blocks_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['core_testnet']
) }}
-- depends_on: {{ ref('bronze_testnet__blocks') }}
WITH base AS (

    SELECT
        DATA,
        DATA :result :block :header AS header,
        header :height :: INT AS block_id,
        header :time :: datetime AS block_timestamp,
        DATA :result :block_id :hash :: STRING AS block_hash,
        COALESCE(ARRAY_SIZE(DATA :result :block :data :txs) :: NUMBER, 0) AS tx_count,
        header :chain_id :: STRING AS chain_id,
        header :proposer_address :: STRING AS proposer_address,
        header :validators_hash :: STRING AS validator_hash
    FROM

{% if is_incremental() %}
{{ ref('bronze_testnet__blocks') }}
WHERE
    inserted_timestamp >= DATEADD(
        MINUTE,
        -5,(
            SELECT
                MAX(
                    modified_timestamp
                )
            FROM
                {{ this }}
        )
    )
{% else %}
    {{ ref('bronze_testnet__blocks_FR') }}
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY chain_id, block_id
ORDER BY
    block_id DESC, inserted_timestamp DESC)) = 1
)
SELECT
    block_id,
    block_timestamp,
    chain_id,
    tx_count,
    proposer_address,
    block_hash,
    validator_hash,
    header,
    DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['chain_id','block_id']
    ) }} AS blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
