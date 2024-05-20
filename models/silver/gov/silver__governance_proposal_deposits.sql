{{ config(
    materialized = 'view',
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
        attribute_key,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        {{ ref('core__fact_msg_attributes') }}
    WHERE
        msg_type IN (
            'proposal_deposit',
            'tx'
        )
),
proposals AS (
    SELECT
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :proposal_id :: INT AS proposal_id,
        j :amount :: STRING AS amount_raw,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    amount_raw,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS amount,
        RIGHT(amount_raw, LENGTH(amount_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(amount_raw, '[^[:digit:]]', ' ')), ' ', 0))) AS denom,
    FROM
        base
    WHERE
        msg_type = 'proposal_deposit'
    GROUP BY
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
),
proposer AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS depositor
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
    depositor,
    p.proposal_id,
    p.amount,
    p.denom,
    {{ dbt_utils.generate_surrogate_key(
        ['p.tx_id']
    ) }} AS governance_proposal_deposits_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    proposals p
    INNER JOIN proposer pp
    ON p.tx_id = pp.tx_id
