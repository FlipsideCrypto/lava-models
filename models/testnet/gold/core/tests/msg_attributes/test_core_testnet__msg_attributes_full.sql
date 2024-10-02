{{ config (
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('core_testnet__fact_msg_attributes') }}
