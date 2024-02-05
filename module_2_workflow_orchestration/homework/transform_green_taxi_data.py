import re
import pandas as pd


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    # Specify your transformation logic here
    print(f'Rows with 0 (Zero) passengers: {data["passenger_count"].isin([0]).sum()}')
    print(f'Rows with 0 (Zero) distance: {data["trip_distance"].isin([0]).sum()}')
    print(f'Existing values of VendorID in the dataset: {data["VendorID"].unique()}')

    # Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    data = data[data['passenger_count'] > 0]
    data = data[data['trip_distance'] > 0]
    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
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
    # vendor_id is one of the existing values in the column (currently)
    assert 'vendor_id' in output.columns, 'vendor_id is missing from columns'
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with 0 (zero) passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with 0 (zero) distance'
