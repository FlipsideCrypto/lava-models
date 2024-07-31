{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "governance_submit_proposal_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore','recent_test']
) }}

WITH base AS (

    SELECT
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        msg_type,
        msg_index,
        attribute_value,
        attribute_key
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type IN (
            'submit_proposal',
            'tx'
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
proposals AS (
    SELECT
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :proposal_id :: INT AS proposal_id,
        j :proposal_messages :: STRING AS proposal_messages
    FROM
        base
    WHERE
        msg_type = 'submit_proposal'
    GROUP BY
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded
),
proposer AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS proposer
    FROM
        base
    WHERE
        msg_type = 'tx'
        AND attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
)
SELECT
    block_id,
    block_timestamp,
    p.tx_id,
    tx_succeeded,
    proposer,
    p.proposal_id,
    p.proposal_messages,
    {{ dbt_utils.generate_surrogate_key(
        ['p.tx_id']
    ) }} AS governance_submit_proposal_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    proposals p
    INNER JOIN proposer pp
    ON p.tx_id = pp.tx_id
