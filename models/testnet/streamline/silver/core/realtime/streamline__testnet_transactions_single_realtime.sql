{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"testnet_tx_search",
        "sql_limit" :"100",
        "producer_batch_size" :"5",
        "worker_batch_size" :"5",
        "exploded_key": "[\"result.txs\"]",
        "sql_source" :"{{this.identifier}}" }
    )
) }}
-- depends_on: {{ ref('streamline__testnet_transactions_complete') }}
-- depends_on: {{ ref('streamline__testnet_tx_counts_complete') }}
WITH blocks AS (

    SELECT
        A.block_number,
        tx_count
    FROM
        {{ ref("streamline__testnet_tx_counts_complete") }} A
    WHERE
        tx_count > 0
        AND block_number = 714114
),
numbers AS (
    SELECT
        _id AS page_number
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence'
        ) }}
    WHERE
        _id < 4770
),
blocks_with_page_numbers AS (
    SELECT
        b.block_number :: INT AS block_number,
        n.page_number
    FROM
        numbers n
        JOIN blocks b
        ON n.page_number <= b.tx_count
    EXCEPT
    SELECT
        block_number,
        page_number
    FROM
        {{ ref("streamline__testnet_transactions_complete") }}
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
                page_number :: STRING,
                '100',
                'asc'
            )
        ),
        'Vault/prod/lava/node/testnet/archive'
    ) AS request,
    page_number,
    block_number AS block_number_requested
FROM
    blocks_with_page_numbers
ORDER BY
    block_number DESC
