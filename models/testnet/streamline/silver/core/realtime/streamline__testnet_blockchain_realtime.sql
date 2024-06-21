{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"testnet_blockchain",
        "sql_limit" :"100",
        "producer_batch_size" :"10",
        "worker_batch_size" :"10",
        "sql_source" :"{{this.identifier}}" }
    )
) }}
-- depends_on: {{ ref('streamline__testnet_blockchain_complete') }}
-- depends_on: {{ ref('silver_testnet__blocks') }}
WITH blocks AS (

    SELECT
        block_id AS block_number
    FROM
        {{ ref("silver_testnet__blocks") }}
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__testnet_blockchain_complete") }}
)
SELECT
    ROUND(
        block_number,
        -4
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
            'blockchain',
            'params',
            ARRAY_CONSTRUCT(
                block_number :: STRING,
                block_number :: STRING
            )
        ),
        'Vault/prod/lava/node/testnet/archive'
    ) AS request
FROM
    blocks
ORDER BY
    block_number
