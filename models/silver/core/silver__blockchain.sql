{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore','recent_test']
) }}
-- depends_on: {{ ref('bronze__blockchain') }}
WITH base AS (

    SELECT
        DATA,
        inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__blockchain') }}
WHERE
    inserted_timestamp >= DATEADD(
        MINUTE,
        -30,(
            SELECT
                MAX(
                    modified_timestamp
                )
            FROM
                {{ this }}
        )
    )
{% else %}
    {{ ref('bronze__blockchain_FR') }}
{% endif %}
),
fin AS (
    SELECT
        VALUE :header :chain_id :: STRING AS chain_id,
        VALUE :header :height :: INT AS height,
        VALUE :header :time :: datetime AS block_timestamp,
        VALUE :block_size :: INT AS block_size,
        VALUE :header AS block_header,
        VALUE :block_id AS block_id,
        VALUE :num_txs :: INT AS num_txs
    FROM
        base,
        LATERAL FLATTEN(
            DATA,
            recursive => TRUE
        ) b
    WHERE
        b.path LIKE 'result.block_metas%'
        AND INDEX IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY height
    ORDER BY
        inserted_timestamp DESC) = 1)
)
SELECT
    chain_id,
    height AS block_id,
    block_timestamp,
    block_size,
    block_header,
    block_id AS block_id_object,
    num_txs,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS blockchain_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin
