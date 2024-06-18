{{ config(
    materialized = 'incremental',
    unique_key = ['address'],
    incremental_strategy = 'merge',
    cluster_by = 'modified_timestamp::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address); DELETE FROM {{ this }} WHERE address in (select address from {{ ref('silver__labels') }} WHERE _is_deleted = TRUE);",
    tags = ['core']
) }}

SELECT
    'lava' AS blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    project_name,
    labels_combined_id AS dim_labels_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__labels') }}

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
