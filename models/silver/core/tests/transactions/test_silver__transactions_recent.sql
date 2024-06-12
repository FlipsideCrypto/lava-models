{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

WITH last_3_days AS (

    SELECT
        block_id
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
    {{ ref('silver__transactions') }}
WHERE
    block_id >= (
        SELECT
            block_id
        FROM
            last_3_days
    )
