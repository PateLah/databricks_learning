-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %run "/Taxi ETL/Green Taxi ETL SQL"

-- COMMAND ----------

-- MAGIC %run "/Taxi ETL/Yellow Taxi ETL SQL"

-- COMMAND ----------

CREATE TABLE if NOT EXISTS TaxiDB.F_Taxi_Gold AS
SELECT 
RideID,
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
Taxi_type
FROM taxidb.greentaxi_silver
UNION ALL
SELECT 
RideID,
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
Taxi_type
FROM taxidb.yellowtaxi_silver




-- COMMAND ----------

SELECT * FROM taxidb.F_taxi_gold limit 100

-- COMMAND ----------


