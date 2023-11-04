-- Databricks notebook source
-- MAGIC %md Yellow Taxi ETL

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS TaxiDB

-- COMMAND ----------

-- MAGIC %md Bronze Table

-- COMMAND ----------


Create temporary view TestTable
using csv
options(
  'path' '/mnt/datalake/Raw/YellowTaxi/*',
  'header' 'true',
  'inferSchema' 'true',
  'delimiter' ';'
);

select * from TestTable limit 30


-- COMMAND ----------

describe TestTable

-- COMMAND ----------

CREATE TABLE if NOT EXISTS TaxiDB.YellowTaxi_Bronze
(
RideId int,
VendorID int,
tpep_pickup_datetime timestamp,
tpep_dropoff_datetime timestamp,
passenger_count int,
trip_distance double,
RatecodeID int,
store_and_fwd_flag string,
PULocationID int,
DOLocationID int,
payment_type int,
fare_amount double,
extra double,
mta_tax double,
tip_amount double,
tolls_amount double,
improvement_surcharge double,
total_amount double,

FileName string,
CreatedOn timestamp
)
USING DELTA
LOCATION "/mnt/datalake/Bronze/Yellowtaxi_Bronze.delta"


-- COMMAND ----------

Copy into TaxiDB.YellowTaxi_Bronze
FROM (
  select 
    RideId::int,
    VendorID::int,
    tpep_pickup_datetime::timestamp,
    tpep_dropoff_datetime::timestamp,
    passenger_count::int,
    trip_distance::double,
    RatecodeID::int,
    store_and_fwd_flag::string,
    PULocationID::int,
    DOLocationID::int,
    payment_type::int,
    fare_amount::double,
    extra::double,
    mta_tax::double,
    tip_amount::double,
    tolls_amount::double,
    improvement_surcharge::double,
    total_amount::double,

    INPUT_FILE_NAME() AS FileName,
    CURRENT_TIMESTAMP() AS CreatedOn
  FROM 'mnt/datalake/Raw/YellowTaxi/*'
)

FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'delimiter' = ';')

-- COMMAND ----------

SELECT * from TaxiDB.YellowTaxi_Bronze limit 30

-- COMMAND ----------

DESCRIBE history TaxiDB.YellowTaxi_Bronze

-- COMMAND ----------

-- MAGIC %md Silver Table

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW BronzeChanges
AS
select 
RideID,
VendorID,

--tpep_pickup_datetime  AS Pickup_Datetime,
--tpep_dropoff_datetime AS Dropoff_Datetime,

DATE(tpep_pickup_datetime)                    AS Dim_Pickup_Date,
date_format(tpep_pickup_datetime, 'HH:mm')    AS Dim_Pickup_Time,
DATE(tpep_dropoff_datetime)                   AS Dim_Dropoff_Date,
date_format(tpep_dropoff_datetime, 'HH:mm')   AS Dim_Dropoff_Time,
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

TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS Time_in_taxi_min,

CASE
  WHEN Passenger_count = 1 THEN "solo"
  WHEN Passenger_count > 1 and Passenger_count <= 5 THEN "shared"
  WHEN Passenger_count > 5 THEN NULL
END                 AS Solo_Shared,
'yellow'             AS Taxi_type


FROM TaxiDB.YellowTaxi_Bronze;

SELECT * FROM BronzeChanges LIMIT 30

-- COMMAND ----------

DESCRIBE BronzeChanges

-- COMMAND ----------

CREATE TABLE if NOT EXISTS TaxiDB.YellowTaxi_Silver
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
Time_in_taxi_min int,
Solo_Shared STRING,
Taxi_type STRING,

CreatedOn timestamp,
ModifiedOn timestamp
)
USING DELTA
LOCATION "/mnt/datalake/Silver/Yellowtaxi_Silver.delta"

-- COMMAND ----------

MERGE into TaxiDB.YellowTaxi_Silver as t 
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
    Time_in_taxi_min,
    Solo_Shared,
    Taxi_type,

    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP())


-- COMMAND ----------


