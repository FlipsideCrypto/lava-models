{{ config(
    materialized = 'view',
    tags = ['noncore','recent_test'],
    enabled = false
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_type,
        msg_index,
        msg_group,
        msg_sub_group,
        attribute_key,
        attribute_value,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type IN (
            'delegate',
            'redelegate',
            'unbond',
            'create_validator',
            {# 'cancel_unbonding_delegation',
            'lava_delegate_to_provider' #},
            'tx',
            {# 'coin_spent', #}
            'message'
        )
),
tx_address AS (
    SELECT
        A.tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_caller_address,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        base A
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY A.tx_id
    ORDER BY
        msg_index)) = 1
),
valid AS (
    SELECT
        block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :delegator :: STRING AS delegator_address,
        COALESCE(
            j :validator :: STRING,
            j :destination_validator :: STRING
        ) AS validator_address,
        j :source_validator :: STRING AS redelegate_source_validator_address,
        j :amount :: STRING AS amount_raw,
        j :completion_time :: STRING AS completion_time,
        j :new_shares :: STRING AS new_shares,
        j: creation_height :: INT AS creation_height,
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
        RIGHT(amount_raw, LENGTH(amount_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(amount_raw, '[^[:digit:]]', ' ')), ' ', 0))) AS currency,
        ROW_NUMBER() over (
            PARTITION BY tx_id,
            msg_group,
            msg_sub_group
            ORDER BY
                msg_index DESC
        ) AS del_rank
    FROM
        base A
    WHERE
        msg_type IN (
            'delegate',
            'redelegate',
            'unbond',
            'create_validator'
        )
    GROUP BY
        block_id,
        A.block_timestamp,
        A.tx_id,
        A.tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type
)
SELECT
    block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    b.tx_caller_address,
    action,
    A.msg_group,
    A.msg_sub_group,
    A.msg_index,
    A.delegator_address,
    A.amount :: INT AS amount,
    A.currency,
    A.validator_address,
    A.redelegate_source_validator_address,
    A.completion_time :: datetime AS completion_time,
    A.creation_height,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id', 'a.msg_index']
    ) }} AS staking_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    valid A
    JOIN tx_address b
    ON A.tx_id = b.tx_id
