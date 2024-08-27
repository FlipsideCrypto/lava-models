{{ config (
    materialized = 'view'
) }}

SELECT
    asset_id,
    'ulava' AS token_address,
    NAME,
    symbol,
    'lava' AS platform,
    platform_id,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_provider_asset_metadata'
    ) }}
WHERE
    asset_id = 'lava-network'
    AND token_address IS NULL
