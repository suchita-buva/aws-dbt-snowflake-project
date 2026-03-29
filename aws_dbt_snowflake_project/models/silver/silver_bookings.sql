-- Incremental model to calculate total booking amount with late-arriving data handling
{{
  config(
      materialized = 'incremental',
      unique_key = 'BOOKING_ID'
  )
}}

SELECT 
    BOOKING_ID,
    LISTING_ID,
    BOOKING_DATE,
    {{ multiply('NIGHTS_BOOKED', 'BOOKING_AMOUNT', 2) }} 
        + CLEANING_FEE 
        + SERVICE_FEE AS TOTAL_AMOUNT,
    BOOKING_STATUS,
    CREATED_AT
FROM 
    {{ ref('bronze_bookings') }}

/*to handle late-arriving updates*/
{% if is_incremental() %}
WHERE CREATED_AT >= DATEADD(day, -7, CURRENT_DATE)
{% endif %}