{{ config(
    materialized = 'view',
    tags = ['noncore','recent_test']
) }}

WITH base_ma AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type,
        attribute_key,
        attribute_value,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        {{ ref('core__fact_msg_attributes') }}
    WHERE
        msg_type IN (
            'message',
            'tx',
            'proposal_vote'
        )
),
tx_mg_msg AS (
    SELECT
        tx_id,
        block_timestamp,
        msg_group,
        msg_sub_group
    FROM
        base_ma
    WHERE
        msg_type = 'message'
        AND attribute_key = 'action'
        AND attribute_value LIKE '%MsgVote'
),
prop_send AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        tx_succeeded,
        A.msg_index,
        A.msg_group,
        A.msg_sub_group,
        A.attribute_key,
        A.attribute_value,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        base_ma A
        JOIN tx_mg_msg b
        ON A.tx_id = b.tx_id
        AND A.msg_group = b.msg_group
        AND A.msg_sub_group = b.msg_sub_group
        AND A.block_timestamp = b.block_timestamp
    WHERE
        msg_type IN (
            'proposal_vote',
            'message'
        )
        AND attribute_key IN (
            'proposal_id',
            'voter',
            'sender'
        )
),
vote_op AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        tx_succeeded,
        A.msg_index,
        A.msg_group,
        A.msg_sub_group,
        A.attribute_key || LEFT(
            b.value,
            3
        ) AS attribute_key,
        REPLACE(REPLACE(b.value, 'option:'), 'weight:') AS attribute_value,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        base_ma A
        LEFT JOIN LATERAL SPLIT_TO_TABLE(
            attribute_value,
            ' '
        ) b
    WHERE
        msg_type = 'proposal_vote'
        AND attribute_key = 'option'
),
vote_msgs AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_index,
        msg_group,
        msg_sub_group,
        attribute_key,
        attribute_value,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        prop_send
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_index,
        msg_group,
        msg_sub_group,
        attribute_key,
        attribute_value,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        vote_op
),
agg AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :proposal_id :: INT AS proposal_id,
        j :voter :: STRING AS voter,
        j :sender :: STRING AS sender,
        j :optionopt :: STRING AS vote_option,
        REPLACE(
            j :optionwei,
            '"'
        ) :: FLOAT AS vote_weight
    FROM
        vote_msgs
    GROUP BY
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_sub_group,
    proposal_id,
    voter AS voter_raw,
    COALESCE(
        voter,
        sender
    ) AS voter,
    vote_option,
    vote_weight,
    {# {{ dbt_utils.generate_surrogate_key(
    ['tx_id','msg_index','voter','vote_option']
) }} AS governance_votes_id,
#}
inserted_timestamp,
modified_timestamp,
_invocation_id
FROM
    agg
