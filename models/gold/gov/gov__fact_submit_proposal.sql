{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
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
        modified_timestamp
    FROM
        {{ ref('silver__governance_submit_proposal') }}
),
bdy AS (
    SELECT
        tx_id,
        TYPE,
        proposer,
        expedited,
        title,
        summary,
        memo,
        details,
        modified_timestamp
    FROM
        {{ ref('silver__governance_submit_proposal_tx_body') }}
)
SELECT
    block_id,
    block_timestamp,
    A.tx_id,
    tx_succeeded,
    COALESCE(
        A.proposer,
        b.proposer
    ) AS proposer,
    proposal_id,
    proposal_messages,
    TYPE,
    expedited,
    title,
    summary,
    memo,
    details,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id']
    ) }} AS fact_submit_proposal_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base A
    LEFT JOIN bdy b
    ON A.tx_id = b.tx_id

{% if is_incremental() %}
WHERE
    GREATEST(
        A.modified_timestamp,
        COALESCE(
            A.modified_timestamp,
            '2000-01-01'
        )
    ) >= DATEADD(
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
