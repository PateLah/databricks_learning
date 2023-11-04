-- Databricks notebook source
-- MAGIC %md Green Taxi ETL

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS TaxiDB

-- COMMAND ----------

-- MAGIC %md Bronze Table

-- COMMAND ----------

/*
Create temporary view TestTable
using csv
options(
  'path' '/mnt/datalake/Raw/GreenTaxi/ tähti',
  'header' 'true',
  'inferSchema' 'true',
  'delimiter' ';'
);

select * from TestTable limit 30
*/

-- COMMAND ----------

CREATE TABLE if NOT EXISTS TaxiDB.GreenTaxi_Bronze
(
RideID int,
VendorID int ,
lpep_pickup_datetime timestamp ,
lpep_dropoff_datetime timestamp ,
store_and_fwd_flag string ,
RatecodeID int ,
PULocationID int ,
DOLocationID int ,
passenger_count int ,
trip_distance double ,
fare_amount double ,
extra double ,
mta_tax double ,
tip_amount double ,
tolls_amount double ,
ehail_fee string ,
improvement_surcharge double ,
total_amount double ,
payment_type int ,
trip_type int,

FileName string,
CreatedOn timestamp
)
USING DELTA
LOCATION "/mnt/datalake/Bronze/Greentaxi_Bronze.delta"


-- COMMAND ----------

Copy into TaxiDB.GreenTaxi_Bronze
FROM (
  select 
    RideID::int,
    VendorID::int ,
    lpep_pickup_datetime::timestamp ,
    lpep_dropoff_datetime::timestamp ,
    store_and_fwd_flag::string ,
    RatecodeID::int ,
    PULocationID::int ,
    DOLocationID::int ,
    passenger_count::int ,
    trip_distance::double ,
    fare_amount::double ,
    extra::double ,
    mta_tax::double ,
    tip_amount::double ,
    tolls_amount::double ,
    ehail_fee::string ,
    improvement_surcharge::double ,
    total_amount::double ,
    payment_type::int ,
    trip_type::int ,

    INPUT_FILE_NAME() AS FileName,
    CURRENT_TIMESTAMP() AS CreatedOn
  FROM 'mnt/datalake/Raw/GreenTaxi/*'
)

FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'delimiter' = ';')

-- COMMAND ----------

SELECT * from TaxiDB.GreenTaxi_Bronze limit 30

-- COMMAND ----------

DESCRIBE history TaxiDB.GreenTaxi_Bronze

-- COMMAND ----------

-- MAGIC %md Silver Table

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW BronzeChanges
AS
select 
RideID,
VendorID,

--lpep_pickup_datetime  AS Pickup_Datetime,
--lpep_dropoff_datetime AS Dropoff_Datetime,

DATE(lpep_pickup_datetime)                    AS Dim_Pickup_Date,
date_format(lpep_pickup_datetime, 'HH:mm')    AS Dim_Pickup_Time,
DATE(lpep_dropoff_datetime)                   AS Dim_Dropoff_Date,
date_format(lpep_dropoff_datetime, 'HH:mm')   AS Dim_Dropoff_Time,
RatecodeID          AS Dim_RatecodeId,
PULocationID        AS Dim_PickupLocation,
DOLocationID        AS Dim_DropoffLocation,
payment_type        AS Dim_Payment_type,
store_and_fwd_flag  AS Store_and_fwd_flag,
CASE
  WHEN passenger_count > 5 THEN NULL
  ELSE passenger_count
END                 AS Passenger_count,
CASE
  WHEN trip_distance = 0 THEN NULL
  ELSE trip_distance
END                 AS Trip_distance,
fare_amount         AS Fare_amount,
extra               AS Extra,
mta_tax             AS Tax,
tip_amount          AS Tip_amount,
tolls_amount        AS Tolls_amount,
improvement_surcharge AS Impr_surcharge,
total_amount        AS Total_amount,
trip_type           AS Trip_type,

TIMESTAMPDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) AS Time_in_taxi_min,

CASE
  WHEN Passenger_count = 1 THEN "solo"
  WHEN Passenger_count > 1 and Passenger_count <= 5 THEN "shared"
  WHEN Passenger_count > 5 THEN NULL
END                 AS Solo_Shared,
'green'             AS Taxi_type


FROM TaxiDB.GreenTaxi_Bronze;

SELECT * FROM BronzeChanges LIMIT 30

-- COMMAND ----------

DESCRIBE BronzeChanges

-- COMMAND ----------

CREATE TABLE if NOT EXISTS TaxiDB.GreenTaxi_Silver
(
RideID int,
VendorID int ,
Dim_Pickup_Date DATE ,
Dim_Pickup_Time STRING ,
Dim_Dropoff_Date DATE ,
Dim_Dropoff_Time STRING ,
Dim_RatecodeId int ,
Dim_PickupLocation int ,
Dim_DropoffLocation int ,
Dim_payment_type int ,
Passenger_count int ,
Trip_distance double ,
Fare_amount double ,
Extra double ,
Tax double ,
Tip_amount double ,
Tolls_amount double ,
Impr_surcharge double ,
Total_amount double ,
Trip_type int,
Time_in_taxi_min int,
Solo_Shared STRING,
Taxi_type STRING,

CreatedOn timestamp,
ModifiedOn timestamp
)
USING DELTA
LOCATION "/mnt/datalake/Silver/Greentaxi_Silver.delta"

-- COMMAND ----------

MERGE into TaxiDB.GreenTaxi_Silver as t 
using BronzeChanges as s on t.RideID = s.RideID
WHEN NOT MATCHED
  THEN
    insert  
    (RideID,
    VendorID,
    Dim_Pickup_Date,
    Dim_Pickup_Time,
    Dim_Dropoff_Date,
    Dim_Dropoff_Time,
    Dim_RatecodeId,
    Dim_PickupLocation,
    Dim_DropoffLocation,
    Dim_payment_type,
    Passenger_count,
    Trip_distance,
    Fare_amount,
    Extra,
    Tax,
    Tip_amount,
    Tolls_amount,
    Impr_surcharge,
    Total_amount,
    Trip_type,
    Time_in_taxi_min,
    Solo_Shared,
    Taxi_type,
    CreatedOn,
    ModifiedOn)
 values
    (RideID,
    VendorID,
    Dim_Pickup_Date,
    Dim_Pickup_Time,
    Dim_Dropoff_Date,
    Dim_Dropoff_Time,
    Dim_RatecodeId,
    Dim_PickupLocation,
    Dim_DropoffLocation,
    Dim_payment_type,
    Passenger_count,
    Trip_distance,
    Fare_amount,
    Extra,
    Tax,
    Tip_amount,
    Tolls_amount,
    Impr_surcharge,
    Total_amount,
    Trip_type,
    Time_in_taxi_min,
    Solo_Shared,
    Taxi_type,

    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP())


-- COMMAND ----------


