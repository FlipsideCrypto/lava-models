{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}
-- depends_on: {{ ref('bronze_testnet__blockchain') }}

SELECT
    DATA :result :block_metas [0] :header :height :: INT AS block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS testnet_complete_blockchain_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    file_name,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze_testnet__blockchain') }}
WHERE
    inserted_timestamp >= (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: DATE)
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze_testnet__blockchain_FR') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY block_number
        ORDER BY
            inserted_timestamp DESC)) = 1
