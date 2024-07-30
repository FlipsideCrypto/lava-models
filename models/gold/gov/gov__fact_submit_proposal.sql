{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "fact_submit_proposal_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} },
    tags = ['noncore','recent_test']
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        proposer,
        proposal_id,
        proposal_messages,
        governance_submit_proposal_id
    FROM
        {{ ref('silver__governance_submit_proposal') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    proposer,
    proposal_id,
    proposal_messages,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS fact_submit_proposal_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
