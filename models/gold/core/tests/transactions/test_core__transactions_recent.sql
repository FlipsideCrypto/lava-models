{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

WITH last_3_days AS (

    SELECT
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
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
    block_timestamp :: DATE >= (
        SELECT
            block_date
        FROM
            last_3_days
    )
