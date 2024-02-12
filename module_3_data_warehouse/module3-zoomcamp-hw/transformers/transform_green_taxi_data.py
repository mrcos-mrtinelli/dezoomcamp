import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    # parse datetime columns to timestamp for BigQuery
    # data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
    # data['lpep_dropoff_datetime'] = pd.to_datetime(data['lpep_dropoff_datetime'])

    data['lpep_pickup_datetime'] =  pd.to_datetime(data['lpep_pickup_datetime'], format='%d%b%Y:%H:%M:%S.%f')
    data['lpep_dropoff_datetime'] =  pd.to_datetime(data['lpep_dropoff_datetime'], format='%d%b%Y:%H:%M:%S.%f')


    # standardize column names
    data = data.rename(columns={
        'VendorID': 'vendor_id',
        'RatecodeID': 'ratecode_id',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id'
    }) 
    data.columns = (
        data.columns
        .str.replace(' ', '_')
        .str.lower()
    )


    return data


@test
def test_output(output, *args) -> None:
    print('no test')
    # is_pickup_datetime_type = pd.api.types.is_datetime64_any_dtype(output['lpep_pickup_datetime'])
    # is_dropoff_datetime_type = pd.api.types.is_datetime64_any_dtype(output['lpep_dropoff_datetime'])

    # assert is_pickup_datetime_type == True, 'lpep_pickup_datetime is NOT datetime'
    # assert is_dropoff_datetime_type == True, 'lpep_dropoff_datetime is NOT datetime'
