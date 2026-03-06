{% set config = [
    {
        "table": "airbnb.gold.obt",
        "columns": "gold_obt.BOOKING_ID, gold_obt.LISTING_ID, gold_obt.HOST_ID, gold_obt.TOTAL_AMOUNT, gold_obt.ACCOMMODATES, gold_obt.BEDROOMS, gold_obt.BATHROOMS, gold_obt.PRICE_PER_NIGHT, gold_obt.RESPONSE_RATE",
        "alias": "gold_obt"
    },
    {
        "table": "airbnb.gold.dim_listings",
        "columns": "",
        "alias": "dim_listings",
        "join_condition": "gold_obt.listing_id = dim_listings.listing_id"
    },
    {
        "table": "airbnb.gold.dim_hosts",
        "columns": "",
        "alias": "dim_hosts",
        "join_condition": "gold_obt.host_id = dim_hosts.host_id"
    }
]
%}

SELECT
    {{config[0]['columns']}}
FROM 
    {% for config in config %}
       {% if loop.first %}
         {{config['table']}} AS {{config['alias']}}
       {% else %}
        LEFT JOIN {{config['table']}} AS {{config['alias']}}
        ON {{config['join_condition']}}
       {% endif %}
    {% endfor %}