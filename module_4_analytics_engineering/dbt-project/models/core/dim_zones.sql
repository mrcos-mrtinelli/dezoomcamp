{{
    config(
        materialized='table'
    )
}}

SELECT 
    locationid,
    borough,
    zone,
    replace(service_zone,'Boro','Green') as service_zone 
FROM 
    {{ ref('taxi_zones_lookup') }}