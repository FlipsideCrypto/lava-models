{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    unique_key = ['tx_id','msg_index','transfer_type'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,sender,receiver);",
    tags = ['core','full_test']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    'LAVA' AS transfer_type,
    msg_index,
    sender,
    receiver,
    amount,
    currency,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index','transfer_type']
    ) }} AS fact_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transfers') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= DATEADD(
        'minute',
        -5,(
            SELECT
                MAX(
                    modified_timestamp
                )
            FROM
                {{ this }}
            WHERE
                transfer_type = 'LAVA'
        )
    )
{% endif %}
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    msg_index,
    sender,
    receiver,
    amount,
    currency,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index','transfer_type']
    ) }} AS fact_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transfers_ibc') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= DATEADD(
        'minute',
        -5,(
            SELECT
                MAX(
                    modified_timestamp
                )
            FROM
                {{ this }}
            WHERE
                transfer_type <> 'LAVA'
        )
    )
{% endif %}
