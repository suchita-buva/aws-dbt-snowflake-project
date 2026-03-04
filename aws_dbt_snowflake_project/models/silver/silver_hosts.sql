{{ config(materialized = 'incremental',unique_key = 'HOST_ID',) }}

SELECT
    HOST_ID,
    HOST_NAME,
    HOST_SINCE,
    IS_SUPERHOST,
    RESPONSE_RATE,
    CASE
        WHEN RESPONSE_RATE > 95 THEN 'VERY GOOD'
        WHEN RESPONSE_RATE > 85 THEN 'GOOD'
        WHEN RESPONSE_RATE > 60 THEN 'FAIR'
        ELSE 'POOR'
    END AS RESPONSE_RATE_QUALITY,
    TO_DATE(CREATED_AT) AS CREATED_AT
FROM 
    {{ ref('bronz_hosts') }}
