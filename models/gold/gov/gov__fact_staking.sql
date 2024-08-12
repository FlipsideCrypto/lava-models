{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "fact_staking_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,tx_caller_address,delegator_address,validator_address);",
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} },
    tags = ['noncore','recent_test']
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        tx_caller_address,
        action,
        msg_group,
        msg_sub_group,
        msg_index,
        delegator_address,
        amount,
        currency,
        validator_address,
        redelegate_source_validator_address,
        completion_time,
        creation_height,
        chain_id,
        from_chain_id,
        to_chain_id,
        provider,
        from_provider,
        to_provider,
        staking_id
    FROM
        {{ ref('silver__staking') }}

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
    tx_caller_address,
    action,
    msg_group,
    msg_sub_group,
    msg_index,
    delegator_address,
    amount,
    currency,
    validator_address,
    redelegate_source_validator_address,
    completion_time,
    creation_height,
    chain_id,
    from_chain_id,
    to_chain_id,
    provider,
    from_provider,
    to_provider,
    staking_id AS fact_staking_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
