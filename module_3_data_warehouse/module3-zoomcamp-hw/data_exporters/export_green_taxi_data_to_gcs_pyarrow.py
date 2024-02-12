import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# set the enviroment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/personal-gcp.json/personal-gcp.json'

bucket_name = 'mage-zoomcamp-practice-bucket'
project_id = 'mage-zoomcamp-413323'
table_name = 'green_taxi_2022_data_parquet'

root_path = f'{bucket_name}'
basename_template = 'green_taxi_2022'

@data_exporter
def export_data(data, *args, **kwargs):
   data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

   table = pa.Table.from_pandas(data)

   gcs = pa.fs.GcsFileSystem()

   pq.write_to_dataset(
    table,
    root_path=root_path,
    filesystem=gcs,
    use_deprecated_int96_timestamps=True
   )