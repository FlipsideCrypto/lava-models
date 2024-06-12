{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_query_v2(
    model = "testnet_tx_search",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER )"
) }}
