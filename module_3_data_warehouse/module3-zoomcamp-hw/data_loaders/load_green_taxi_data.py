import io
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

    appended_taxi_data = []

    for month in range(1,13):
        url_file_name = f'green_tripdata_2022-{month:02d}.parquet'
        df = pd.read_parquet(f'{base_url}{url_file_name}')
        appended_taxi_data.append(df)
    
    # join/concat dfs
    appended_taxi_data = pd.concat(appended_taxi_data)

    return appended_taxi_data


@test
def test_output(output, *args) -> None:
    print(f'Number of columns in dataset: {len(output.columns)}')
    assert len(output.columns) == 20, 'The output is undefined'
