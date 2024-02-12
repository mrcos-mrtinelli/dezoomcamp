# Setup Mage
1. Create `Docker` file to pull latest mage
2. Create `docker-compose.yaml` file with container settings
3. Build container
4. Run container

## Data Loader

1. Use `pandas` to import the parquet data one URL/File at time.
2. Create DataFram from each and append them to a list
3. Append the list together to create a single DataFrame with all of the data.

Code: [load_green_taxi_data.py](https://github.com/mrcos-mrtinelli/dezoomcamp/blob/main/module_3_data_warehouse/module3-zoomcamp-hw/data_loaders/load_green_taxi_data.py)

## Data Tranformer

None used this time.

## Data Exporter 

1. Use `pyarrow` to export data to Google Cloud Storage to avoid issues with data types/parquet schema.

Code: [export_green_taxi_data_to_gcs_pyarrow](https://github.com/mrcos-mrtinelli/dezoomcamp/blob/main/module_3_data_warehouse/module3-zoomcamp-hw/data_exporters/export_green_taxi_data_to_gcs_pyarrow.py)

# Homework Answers:
[Homework repo](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/03-data-warehouse/homework.md)

## Setup
Create an external table using the Green Taxi Trip Records Data for 2022.
```
CREATE OR REPLACE EXTERNAL TABLE `mage-zoomcamp-413323.green_taxi_2022_dataset.green_taxi_external` 
OPTIONS (
  format = 'parquet',
  uris = ['gs://mage-zoomcamp-practice-bucket/green_taxi_2022_data.parquet']
);
```
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
```
CREATE OR REPLACE TABLE `mage-zoomcamp-413323.green_taxi_2022_dataset.green_taxi`;

LOAD DATA OVERWRITE `mage-zoomcamp-413323.green_taxi_2022_dataset.green_taxi`
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-practice-bucket/green_taxi_2022_data.parquet']);
```

**Question 1:** What is count of records for the 2022 Green Taxi Data??

**Answer:** 840,402

**Question 2:** Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

**Answer:** 0 MB for the External Table and 6.41MB for the Materialized Table

```
SELECT DISTINCT(pu_location_id) FROM `mage-zoomcamp-413323.green_taxi_2022_dataset.green_taxi`;

SELECT DISTINCT(pu_location_id) FROM `mage-zoomcamp-413323.green_taxi_2022_dataset.green_taxi_external`;
```

**Question 3:** How many records have a fare_amount of 0?

**Answer:** 1,622

```
SELECT COUNT(*) FROM `mage-zoomcamp-413323.green_taxi_2022_dataset.green_taxi_external`
WHERE fare_amount = 0;
```

**Question 4:** What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

**Answer:** Partition by lpep_pickup_datetime Cluster on PUlocationID

```
CREATE OR REPLACE TABLE `mage-zoomcamp-413323.green_taxi_2022_dataset.green_taxi_partitioned`
PARTITION BY
    DATE(lpep_pickup_datetime)
CLUSTER BY pu_location_id AS
SELECT * FROM `green_taxi_2022_dataset.green_taxi_external`;
```
**Question 5:** Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

**Answer:** 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

```
SELECT DISTINCT(PULocationID) FROM `mage-zoomcamp-413323.green_taxi_2022_dataset.green_taxi_partitioned`
WHERE CAST(lpep_pickup_datetime AS DATE) BETWEEN '2022-06-01' and '2022-06-30'
```

**Question 6:** Where is the data stored in the External Table you created?

**Answer:** GCP Bucket

**Question 7:** It is best practice in Big Query to always cluster your data:

**Answer:** False

**Bonus: Not worth points Question 8:** 
No Points: Write a SELECT count(*) query FROM the materialized table you created. 

How many bytes does it estimate will be read? Why?

**Answer:** 120.52 MB because the query will run through the entire table stored in BigQuery.

```
SELECT * FROM `mage-zoomcamp-413323.green_taxi_2022_dataset.green_taxi`;
```
