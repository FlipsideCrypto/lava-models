{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['noncore','recent_test'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    }} }
) }}
-- depends_on: {{ ref('core__fact_blocks') }}
-- depends_on: {{ ref('core__fact_transactions') }}
{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    MIN(DATE_TRUNC('hour', block_timestamp)) block_timestamp_hour
FROM
    {{ ref('core__fact_blocks') }}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_block_timestamp_hour_blocks = run_query(query).columns [0].values() [0] %}
    {% set query2 %}
SELECT
    MIN(DATE_TRUNC('hour', block_timestamp)) block_timestamp_hour
FROM
    {{ ref('core__fact_transactions') }}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_block_timestamp_hour_txns = run_query(query2).columns [0].values() [0] %}
{% endif %}
{% endif %}

WITH txs AS (
    SELECT
        block_timestamp_hour,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count,
        total_fees AS total_fees_native,
        price AS imputed_close,
        core_metrics_hourly_id AS ez_core_metrics_hourly_id,
        s.inserted_timestamp AS inserted_timestamp,
        s.modified_timestamp AS modified_timestamp
    FROM
        {{ ref('silver_stats__core_metrics_hourly') }}
        s
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON s.block_timestamp_hour = p.hour
        AND p.token_address = 'ulava'

{% if is_incremental() %}
WHERE
    block_timestamp_hour >= LEAST(
        COALESCE(
            '{{ min_block_timestamp_hour_blocks }}',
            '2000-01-01'
        ),
        COALESCE(
            '{{ min_block_timestamp_hour_txns }}',
            '2000-01-01'
        )
    )
{% endif %}
),
bks AS (
    SELECT
        A.block_timestamp_hour,
        A.block_number_min,
        A.block_number_max,
        A.block_count,
        A.core_metrics_block_hourly_id,
        A.inserted_timestamp,
        A.modified_timestamp
    FROM
        {{ ref('silver_stats__core_metrics_block_hourly') }} A

{% if is_incremental() %}
WHERE
    block_timestamp_hour >= LEAST(
        COALESCE(
            '{{ min_block_timestamp_hour_blocks }}',
            '2000-01-01'
        ),
        COALESCE(
            '{{ min_block_timestamp_hour_txns }}',
            '2000-01-01'
        )
    )
{% endif %}
)
SELECT
    A.block_timestamp_hour,
    A.block_number_min,
    A.block_number_max,
    A.block_count,
    b.transaction_count,
    b.transaction_count_success,
    b.transaction_count_failed,
    b.unique_from_count,
    b.total_fees_native,
    ROUND(
        b.total_fees_native * b.imputed_close,
        2
    ) AS total_fees_usd,
    A.core_metrics_block_hourly_id AS ez_core_metrics_hourly_id,
    GREATEST(
        A.inserted_timestamp,
        b.inserted_timestamp
    ) AS inserted_timestamp,
    GREATEST(
        A.modified_timestamp,
        b.modified_timestamp
    ) AS modified_timestamp
FROM
    bks A
    JOIN txs b
    ON A.block_timestamp_hour = b.block_timestamp_hour
