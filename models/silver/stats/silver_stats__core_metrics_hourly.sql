{{ config(
    materialized = 'view',
    tags = ['noncore','recent_test']
) }}

SELECT
    DATE_TRUNC(
        'hour',
        block_timestamp
    ) AS block_timestamp_hour,
    COUNT(
        DISTINCT tx_id
    ) AS transaction_count,
    COUNT(
        DISTINCT CASE
            WHEN tx_succeeded THEN tx_id
        END
    ) AS transaction_count_success,
    COUNT(
        DISTINCT CASE
            WHEN NOT tx_succeeded THEN tx_id
        END
    ) AS transaction_count_failed,
    COUNT(
        DISTINCT tx_from
    ) AS unique_from_count,
    SUM(
        fee / pow(
            10,
            6
        )
    ) AS total_fees,
    {{ dbt_utils.generate_surrogate_key(
        ['block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    MAX(inserted_timestamp) AS inserted_timestamp,
    MAX(modified_timestamp) AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('core__fact_transactions') }}
WHERE
    block_timestamp_hour < DATE_TRUNC('hour', systimestamp())
GROUP BY
    1
