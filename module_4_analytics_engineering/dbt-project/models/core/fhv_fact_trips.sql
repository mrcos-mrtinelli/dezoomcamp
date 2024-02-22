{{
    config(
        materialized='table'
    )
}}

with fhv_trips_data as (
    select 
        *,
        'For-Hire' as service_type
    from {{ ref("stg_fhv_trips") }}
),
dim_zones_data as (
    select 
        *
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv_trips_data.tripid,
    fhv_trips_data.base_num,
    fhv_trips_data.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_trips_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone, 
    fhv_trips_data.pickup_datetime,
    fhv_trips_data.dropoff_datetime,
    fhv_trips_data.sr_flag,
    fhv_trips_data.affiliated_base_num,
    fhv_trips_data.service_type
from fhv_trips_data
inner join dim_zones_data as pickup_zone on fhv_trips_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones_data as dropoff_zone on fhv_trips_data.pickup_locationid = dropoff_zone.locationid