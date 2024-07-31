{{ config(
    materialized = 'incremental',
    unique_key = "governance_submit_proposal_tx_body_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore','recent_test']
) }}

WITH base AS (

    SELECT
        tx_id,
        DATA
    FROM
        {{ ref('bronze_api__gov_prop_body') }}
    WHERE
        DATA :error IS NULL

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
)
SELECT
    tx_id,
    COALESCE(
        DATA :data :tx :body :messages [0] :content :"@type",
        DATA :data :tx :body :messages [0] :messages [0] :"@type"
    ) :: STRING AS TYPE,
    DATA :data :tx :body :messages [0] :proposer :: STRING AS proposer,
    COALESCE(
        DATA :data :tx :body :messages [0] :expedited,
        FALSE
    ) AS expedited,
    COALESCE(
        DATA :data :tx :body :messages [0] :title,
        DATA :data :tx :body :messages [0] :content :title
    ) :: STRING AS title,
    COALESCE(
        DATA :data :tx :body :messages [0] :summary,
        DATA :data :tx :body :messages [0] :content :description
    ) :: STRING AS summary,
    DATA :data :tx :body :memo :: STRING AS memo,
    COALESCE(
        DATA :data :tx :body :messages [0] :messages [0],
        DATA :data :tx :body :messages [0] :content
    ) AS details,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS governance_submit_proposal_tx_body_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    modified_timestamp DESC)) = 1
