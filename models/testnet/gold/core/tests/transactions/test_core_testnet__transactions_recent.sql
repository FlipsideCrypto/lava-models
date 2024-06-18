{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

WITH last_3_days AS (

    SELECT
        block_id
    FROM
        {{ ref("_testnet_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_id DESC
        ) = 3
)
SELECT
    *
FROM
    {{ ref('core__fact_transactions') }}
WHERE
    block_id >= (
        SELECT
            block_id
        FROM
            last_3_days
    )
