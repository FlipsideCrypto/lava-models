{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    1356509 block_number {# SELECT
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'livequery'
        ),
        OBJECT_CONSTRUCT(
            'id',
            0,
            'jsonrpc',
            '2.0',
            'method',
            'status',
            'params',
            []
        ),
        'Vault/prod/lava/node/testnet/rpc'
    ) :data :result :sync_info :latest_block_height :: INT AS block_number #}
