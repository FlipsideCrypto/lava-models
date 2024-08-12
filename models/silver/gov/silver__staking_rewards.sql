{{ config(
    materialized = 'view',
    tags = ['noncore','recent_test']
) }}

WITH msg_attributes AS (

    SELECT
        tx_id,
        msg_type,
        msg_index,
        attribute_key,
        attribute_value,
        block_id,
        block_timestamp,
        tx_succeeded,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        {{ ref('silver__msg_attributes') }} A
    WHERE
        msg_type IN(
            'withdraw_rewards',
            'tx'
        )
),
reroll_msg AS (
    SELECT
        tx_id,
        msg_type,
        msg_index,
        block_id,
        block_timestamp,
        tx_succeeded,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS attributes,
        attributes :amount :: STRING AS amount_raw,
        attributes :validator :: STRING AS validator_address,
        attributes :delegator :: STRING AS delegator_address
    FROM
        msg_attributes
    WHERE
        msg_type = 'withdraw_rewards'
    GROUP BY
        tx_id,
        msg_type,
        msg_index,
        block_id,
        block_timestamp,
        tx_succeeded,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
),
tx_address AS (
    SELECT
        A.tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_caller_address,
        SPLIT_PART(
            attribute_value,
            '/',
            1
        ) AS acc_seq_index
    FROM
        msg_attributes A
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over (PARTITION BY tx_id
    ORDER BY
        acc_seq_index) = 1)
),
prefinal AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        C.tx_caller_address,
        A.msg_index,
        A.delegator_address,
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
        ) :: INT AS amount,
        RIGHT(amount_raw, LENGTH(amount_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(amount_raw, '[^[:digit:]]', ' ')), ' ', 0))) AS currency,
        A.validator_address,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        reroll_msg A
        JOIN tx_address C
        ON A.tx_id = C.tx_id
)
SELECT
    block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    A.tx_caller_address,
    A.msg_index,
    A.delegator_address,
    A.amount,
    A.currency,
    A.validator_address,
    'withdraw_rewards' AS action,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index']
    ) }} AS staking_rewards_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    prefinal A
