{{ config (
    materialized = "ephemeral",
    unique_key = "block_id",
) }}

WITH base AS (

    SELECT
        block_timestamp :: DATE AS block_date,
        MAX(block_id) AS block_id
    FROM
        {{ ref("silver__blocks") }}
    GROUP BY
        block_timestamp :: DATE
)
SELECT
    block_date,
    block_id
FROM
    base
WHERE
    block_date <> (
        SELECT
            MAX(block_date)
        FROM
            base
    )
