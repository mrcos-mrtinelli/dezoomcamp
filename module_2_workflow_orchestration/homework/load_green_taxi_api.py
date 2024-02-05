import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    # urls for data
    urls = [
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz'
    ]
    # set data types
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'store_and_fwd_flag':str,
        'RatecodeID':pd.Int64Dtype(),
        'PULocationID':pd.Int64Dtype(),
        'DOLocationID':pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'fare_amount': float,
        'extra':float,
        'mta_tax':float,
        'tip_amount':float,
        'tolls_amount':float,
        'ehail_fee':float,
        'improvement_surcharge':float,
        'total_amount':float,
        'payment_type': pd.Int64Dtype(),
        'trip_type':float,
        'congestion_surcharge':float
    }
    # date parsing
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    # 
    appended_data = []
    for url in urls:
        df = pd.read_csv(url, dtype=taxi_dtypes, parse_dates=parse_dates) #, iterator=True
        appended_data.append(df)

    appended_data = pd.concat(appended_data)

    return appended_data

