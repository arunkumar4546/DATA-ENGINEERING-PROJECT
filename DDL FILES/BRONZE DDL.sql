-- Databricks notebook source
-- DBTITLE 1,Cell 1

create table miniproject.bronze.driver (driver_id varchar(20),driver_name varchar(20),vehicle_id varchar(20),experience_years varchar(20),ingestion_date date,ingestion_time timestamp);

-- COMMAND ----------

create table miniproject.bronze.vehicle (vehicle_id varchar(20),vehicle_type varchar(20),capacity varchar(20),depot varchar(20),ingestion_date date,ingestion_time timestamp);

-- COMMAND ----------

create table miniproject.bronze.route (route_id varchar(20),source varchar(20),destination varchar(20),distance_km varchar(20),ingestion_date date,ingestion_time timestamp);

-- COMMAND ----------

create table miniproject.bronze.trip (trip_id varchar(20),driver_id varchar(20),vehicle_id varchar(20),route_id varchar(20),trip_date varchar(20),passengers varchar(20),revenue varchar(20),ingestion_date date,ingestion_time timestamp);