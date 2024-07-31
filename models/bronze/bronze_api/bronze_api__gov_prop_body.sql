{{ config(
    materialized = 'incremental',
    full_refresh = false,
    tags = ['noncore']
) }}

WITH props AS (

    SELECT
        tx_id
    FROM
        {{ ref('silver__governance_submit_proposal') }} A

{% if is_incremental() %}
WHERE
    modified_timestamp >= DATEADD(
        'minute',
        -5,(
            SELECT
                MAX(
                    modified_timestamp
                )
            FROM
                {{ this }}
        )
    )
{% endif %}
)
SELECT
    tx_id,
    {{ target.database }}.live.udf_api(
        'get',
        'https://lava.rest.lava.build/cosmos/tx/v1beta1/txs/' || tx_id,
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),{},
        'Vault/prod/lava/node/mainnet'
    ) AS DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS gov_prop_body_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    props
