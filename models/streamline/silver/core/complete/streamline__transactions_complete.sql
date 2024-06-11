{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "complete_transactions_id",
    cluster_by = "ROUND(block_number, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}
-- depends_on: {{ ref('bronze__transactions') }}

SELECT
    DATA :height :: INT AS block_number,
    VALUE :PAGE_NUMBER :: INT AS page_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number','page_number']
    ) }} AS complete_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    file_name,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__transactions') }}
{% else %}
    {{ ref('bronze__transactions_FR') }}
{% endif %}
WHERE
    DATA <> '[]'

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(modified_timestamp) modified_timestamp
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY complete_transactions_id
ORDER BY
    inserted_timestamp DESC)) = 1
