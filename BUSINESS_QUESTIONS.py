# Databricks notebook source
# MAGIC %md
# MAGIC Which routes generate the highest revenue?

# COMMAND ----------

# DBTITLE 1,Cell 1
spark.sql("""
SELECT 
    r.route_id,
    r.source,
    r.destination,
    COUNT(f.trip_id) AS total_trips,
    SUM(f.passengers) AS total_passengers,
    SUM(f.revenue) AS total_revenue
FROM miniproject.gold.fact_trip f
JOIN miniproject.gold.dim_route r
ON f.route_sk = r.route_sk
GROUP BY r.route_id, r.source, r.destination
ORDER BY total_revenue DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Who are the top-performing drivers?

# COMMAND ----------

spark.sql("""
SELECT 
    d.driver_id,
    d.driver_name,
    COUNT(f.trip_id) AS total_trips,
    SUM(f.passengers) AS total_passengers,
    SUM(f.revenue) AS total_revenue
FROM miniproject.gold.fact_trip f
JOIN miniproject.gold.dim_driver d
ON f.driver_sk = d.driver_sk
GROUP BY d.driver_id, d.driver_name
ORDER BY total_revenue DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Which vehicles are most used?

# COMMAND ----------

spark.sql("""
SELECT 
    v.vehicle_id,
    v.vehicle_type,
    COUNT(f.trip_id) AS total_trips,
    SUM(f.passengers) AS total_passengers,
    SUM(f.revenue) AS total_revenue
FROM miniproject.gold.fact_trip f
JOIN miniproject.gold.dim_vehicle v
ON f.vehicle_sk = v.vehicle_sk
GROUP BY v.vehicle_id, v.vehicle_type
ORDER BY total_trips DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC How is business performing daily?

# COMMAND ----------

spark.sql("""
SELECT 
    trip_date,
    COUNT(trip_id) AS total_trips,
    SUM(passengers) AS total_passengers,
    SUM(revenue) AS total_revenue
FROM miniproject.gold.fact_trip
GROUP BY trip_date
ORDER BY total_revenue DESC
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_trips,
# MAGIC     SUM(passengers) AS total_passengers,
# MAGIC     SUM(revenue) AS total_revenue,
# MAGIC     ROUND(SUM(passengers)/COUNT(*),2) AS avg_passengers_per_trip
# MAGIC FROM miniproject.gold.fact_trip