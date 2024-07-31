{{ config (
    materialized = 'view',
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref('core_testnet__fact_msgs') }}
