{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"blockchain",
        "sql_limit" :"100000",
        "producer_batch_size" :"100000",
        "worker_batch_size" :"1000",
        "sql_source" :"{{this.identifier}}" }
    )
) }}
-- depends_on: {{ ref('streamline__complete_blockchain') }}
WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_blocks") }}
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_blockchain") }}
    ORDER BY
        1
    LIMIT
        50000
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
        'Vault/prod/lava/node/mainnet'
    ) AS request
FROM
    blocks
ORDER BY
    block_number
