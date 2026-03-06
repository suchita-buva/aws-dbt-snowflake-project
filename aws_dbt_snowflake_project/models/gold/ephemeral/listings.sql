{{
  config(
    materialized = 'ephemeral',
    )
}}

WITH LISTINGS AS
(
    SELECT
        LISTING_ID,
        PROPERTY_TYPE,
        ROOM_TYPE,
        CITY,
        COUNTRY,
        PRICE_TAG,
        LISTINGS_CREATED_AT
    FROM {{ ref('obt') }}    
)
SELECT * FROM listings