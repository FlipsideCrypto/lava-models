{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core_testnet','full_test']
) }}

WITH atts AS (

    SELECT
        tx_id,
        msg_index,
        attribute_key,
        attribute_value,
        modified_timestamp
    FROM
        {{ ref('core_testnet__fact_msg_attributes') }}
    WHERE
        msg_type = 'tx'
        AND attribute_key IN (
            'fee',
            'acc_seq'
        )
),
fee AS (
    SELECT
        tx_id,
        attribute_value AS fee_raw,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    fee_raw,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS fee,
        RIGHT(fee_raw, LENGTH(fee_raw) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(fee_raw, '[^[:digit:]]', ' ')), ' ', 0))) AS fee_denom,
        modified_timestamp
    FROM
        atts
    WHERE
        attribute_key = 'fee' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
),
spender AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS tx_from,
        modified_timestamp
    FROM
        atts
    WHERE
        attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        msg_index)) = 1
)
SELECT
    t.block_id,
    b.block_timestamp,
    t.tx_id,
    s.tx_from,
    tx_succeeded,
    NULLIF(
        codespace,
        ''
    ) AS codespace,
    COALESCE(TRY_CAST(fee AS INT), 0) AS fee,
    COALESCE(NULLIF(fee_denom, ''), 'ulava') AS fee_denom,
    gas_used,
    gas_wanted,
    tx_code,
    msgs,
    tx_log,
    {{ dbt_utils.generate_surrogate_key(
        ['t.tx_id']
    ) }} AS transactions_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver_testnet__transactions') }}
    t
    LEFT OUTER JOIN {{ ref('silver_testnet__blocks') }}
    b
    ON t.block_id = b.block_id
    LEFT OUTER JOIN fee f
    ON t.tx_id = f.tx_id
    LEFT OUTER JOIN spender s
    ON t.tx_id = s.tx_id

{% if is_incremental() %}
WHERE
    GREATEST(
        t.modified_timestamp,
        b.modified_timestamp,
        f.modified_timestamp,
        s.modified_timestamp
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
