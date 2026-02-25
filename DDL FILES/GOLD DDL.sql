# COMMAND ----------

# DBTITLE 1,Cell 1
create table miniproject.gold.dim_driver(
driver_sk bigint generated always as identity,
driver_id string,
driver_name string,
vehicle_id string,
experience_years int,
hash_key string,
is_active boolean,
effective_start_date date,
effective_end_date date
)

# COMMAND ----------

create table miniproject.gold.dim_route(
route_sk bigint generated always as identity,
route_id string,
source string,
destination string,
distance_km int,
hash_key string,
is_active boolean,
effective_start_date date,
effective_end_date date
)

# COMMAND ----------

create table miniproject.gold.dim_vehicle(
vehicle_sk bigint generated always as identity,
vehicle_id string,
vehicle_type string,
capacity int,
depot string,

hash_key string,
is_active boolean,
effective_start_date date,
effective_end_date date
)

# COMMAND ----------


create table miniproject.gold.fact_trip(
driver_sk bigint,
vehicle_sk bigint,
route_sk bigint,
trip_id string,
trip_date date,
passengers int, 
revenue int
)