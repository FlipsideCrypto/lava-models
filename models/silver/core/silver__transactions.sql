{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.partition_key >= (select min(partition_key) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    tags = ['core','full_test']
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
WITH base_table AS (

    SELECT
        DATA :height :: INT AS block_id,
        DATA :hash :: STRING AS tx_id,
        DATA :index AS tx_index,
        DATA :tx_result :codespace :: STRING AS codespace,
        DATA :tx_result :gas_used :: NUMBER AS gas_used,
        DATA :tx_result :gas_wanted :: NUMBER AS gas_wanted,
        DATA :tx_result :code :: INT AS tx_code,
        CASE
            WHEN NULLIF(
                tx_code,
                0
            ) IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS tx_succeeded,
        DATA :tx_result :events AS msgs,
        COALESCE(
            TRY_PARSE_JSON(
                DATA :tx_result :log
            ),
            DATA :tx_result :log
        ) AS tx_log,
        TRY_BASE64_DECODE_STRING(
            DATA :tx_result :data
        ) AS tx_type,
        DATA,
        partition_key
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}
WHERE
    tx_id IS NOT NULL

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    block_id DESC, inserted_timestamp DESC)) = 1
)
SELECT
    block_id,
    tx_id,
    tx_index,
    tx_succeeded,
    tx_code,
    codespace,
    gas_used,
    gas_wanted,
    tx_type,
    msgs,
    tx_log AS tx_log,
    DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS transactions_id,
    partition_key,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_table
