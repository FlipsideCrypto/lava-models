{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    unique_key = ['tx_id','msg_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','recent_test']
) }}

WITH base_atts AS (

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
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type IN (
            'send_packet',
            'acknowledge_packet',
            'timeout_packet'
        )
        AND attribute_key IN (
            'packet_sequence',
            'packet_src_channel',
            'packet_dst_channel'
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
all_data AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        j :packet_sequence :: STRING AS packet_sequence,
        j :packet_src_channel :: STRING AS packet_src_channel,
        j :packet_dst_channel :: STRING AS packet_dst_channel
    FROM
        base_atts
    GROUP BY
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        msg_group,
        msg_sub_group,
        msg_index,
        msg_type
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_sub_group,
    msg_index,
    msg_type,
    packet_sequence,
    packet_src_channel,
    packet_dst_channel,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index']
    ) }} AS transfers_ibc_packets_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_data
