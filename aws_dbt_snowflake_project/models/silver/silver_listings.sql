{{config(materialized = 'incremental',unique_key='LISTING_ID')}}

SELECT 
    LISTING_ID,
    HOST_ID,
    {{trimmer('PROPERTY_TYPE')}} AS PROPERTY_TYPE,
    ROOM_TYPE,
    CITY,
    COUNTRY,
    ACCOMMODATES,
    BEDROOMS,
    BATHROOMS,
    PRICE_PER_NIGHT,
    CREATED_AT,
    {{tag('CAST(PRICE_PER_NIGHT AS INT)')}} AS PRICE_TAG
FROM 
{{ ref('bronze_listings') }}

/*to handle late-arriving updates*/
{% if is_incremental() %}
WHERE CREATED_AT >= DATEADD(day, -7, CURRENT_DATE)
{% endif %}