{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"testnet_txcount",
        "sql_limit" :"500",
        "producer_batch_size" :"10",
        "worker_batch_size" :"10",
        "sql_source" :"{{this.identifier}}" }
    )
) }}
-- depends_on: {{ ref('streamline__testnet_tx_counts_complete') }}
WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__testnet_blocks") }}
    WHERE
        block_number > 1970000
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__testnet_tx_counts_complete") }}
),
{# retry AS (
SELECT
    NULL AS A.block_number
FROM
    {{ ref("streamline__tx_counts") }} A
    JOIN {{ ref("silver__blockchain") }}
    b
    ON A.block_number = b.block_id
WHERE
    A.tx_count <> b.num_txs
),
#}
combo AS (
    SELECT
        block_number
    FROM
        blocks {# UNION
    SELECT
        block_number
    FROM
        retry #}
)
SELECT
    ROUND(
        block_number,
        -3
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'tx_search',
            'params',
            ARRAY_CONSTRUCT(
                'tx.height=' || block_number :: STRING,
                TRUE,
                '1',
                '1',
                'asc'
            )
        ),
        'Vault/prod/lava/node/testnet/rpc'
    ) AS request,
    block_number
FROM
    combo
ORDER BY
    block_number
