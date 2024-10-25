{{ config (
    materialized = 'view'
) }}

SELECT
    'ulava' AS token_address,
    asset_id,
    symbol,
    'Lava Network' AS NAME,
    decimals,
    'lava' AS blockchain,
    'lava' AS blockchain_name,
    blockchain_id,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_token_asset_metadata'
    ) }}
WHERE
    asset_id = 'lava-network' qualify(ROW_NUMBER() over(PARTITION BY asset_id
ORDER BY
    is_deprecated, blockchain_id) = 1)
