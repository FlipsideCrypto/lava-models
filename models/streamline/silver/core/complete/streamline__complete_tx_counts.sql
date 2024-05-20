-- depends_on: {{ ref('bronze__streamline_tx_counts') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}

SELECT
    VALUE :BLOCK_NUMBER :: INT AS block_number,
    DATA :result :total_count :: INT AS tx_count,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS complete_tx_counts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    file_name,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_tx_counts') }}
WHERE
    inserted_timestamp >= (
        SELECT
            MAX(modified_timestamp) modified_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_FR_tx_counts') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    inserted_timestamp DESC)) = 1
