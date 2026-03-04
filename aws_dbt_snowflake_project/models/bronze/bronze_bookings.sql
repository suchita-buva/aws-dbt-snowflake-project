/* to load only extra updated data in case of incremental data */
{{config(materialized = 'incremental',)}}

/* main query to load data into bronze.bookings table from staging schema */
SELECT * FROM {{ source('staging', 'bookings') }}

/* to load only incremental data using date column */
{% if is_incremental() %}
    WHERE CREATED_AT >= DATEADD(day, -7, CURRENT_DATE)
{% endif %}