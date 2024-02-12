-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `bq_for_dw_dataset.green_taxi_2022_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gcs-for-dw-411900-terra-bucket/green_tripdata_2022-*.parquet']
);


-- Question 1: What is count of records for the 2022 Green Taxi Data??
SELECT COUNT(1) FROM `bq_for_dw_dataset.green_taxi_2022_data`;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE bq_for_dw_dataset.green_taxi_2022_data_non_partitioned AS
SELECT * FROM bq_for_dw_dataset.green_taxi_2022_data;


/* Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
   What is the estimated amount of data that will be read when this query is executed on the External Table and the Table? */

SELECT COUNT(DISTINCT PULocationID) FROM bq_for_dw_dataset.green_taxi_2022_data;

SELECT COUNT(DISTINCT PULocationID) FROM bq_for_dw_dataset.green_taxi_2022_data_non_partitioned;


-- Question 3: How many records have a fare_amount of 0?
SELECT COUNT(1) 
FROM bq_for_dw_dataset.green_taxi_2022_data 
WHERE fare_amount = 0;


-- Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE bq_for_dw_dataset.green_taxi_2022_data_partitioned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM bq_for_dw_dataset.green_taxi_2022_data;


/* Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
   Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? */

SELECT COUNT(DISTINCT PULocationID) 
FROM bq_for_dw_dataset.green_taxi_2022_data_non_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';


SELECT COUNT(DISTINCT PULocationID) 
FROM bq_for_dw_dataset.green_taxi_2022_data_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';