
from pyspark.sql.functions import *

create table miniproject.silver.route(
   route_id string,
   source string,
   destination string,
   distance_km int,

   hash_key string
 )

# COMMAND ----------


create table miniproject.silver.vehicle(
   vehicle_id string,
   vehicle_type string,
   capacity int,
   depot string,

   hash_key string
 )

# COMMAND ----------


 create table miniproject.silver.driver(
   driver_id string,
   driver_name string,
   vehicle_id string,
   experience_years int,

   hash_key string
 )

# COMMAND ----------


 create table miniproject.silver.trip(
   trip_id string,
   driver_id string,
   vehicle_id string,
   route_id string,
   trip_date date,
   passengers int, 
   revenue int,

   hash_key string
 )