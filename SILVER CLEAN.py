# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Cell 2
audit_table = "miniproject.meta_table.audit_table"
table_name = "driver"
bronze_table = "miniproject.bronze.driver"
try:
    last_load_ts = spark.sql(f"""
    SELECT MAX(last_load_timestamp)
    FROM {audit_table}
    WHERE table_name = '{table_name}'
    AND layer = 'silver'
    AND status = 'SUCCESS'
    """).collect()[0][0]
    
    if last_load_ts is None:
        bronze_df = spark.read.table(bronze_table)
    else:
        bronze_df = spark.read.table(bronze_table) \
            .filter(col("ingestion_time") > last_load_ts)
    bronze_count = bronze_df.count()

    if bronze_count == 0:
        print("No new data")

        spark.sql(f"""
        INSERT INTO {audit_table}
        (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
        VALUES (
            '{table_name}',
            'silver',
            'INCREMENTAL',
            current_timestamp(),
            current_timestamp(),
            0,
            'NO_DATA',
            'No new records'
        )
        """)

        pass
    else:
        driver=bronze_df.select("driver_id", "driver_name", "vehicle_id", "experience_years")
        driver_clean=(driver
                    .withColumn("driver_name",initcap(col("driver_name")))
                    .withColumn("experience_years",col("experience_years").cast("integer"))
                    .fillna({"driver_name": "Unknown", "experience_years": 0})
                    .filter(
                        (col("driver_id").isNotNull()) & 
                        (col("vehicle_id").isNotNull()) & 
                        (col("experience_years")>=0)
                    )
        )

        driver_final=(
            driver_clean
            .withColumn(
                "hash_key",
                sha2(concat_ws("||",
                            col("driver_name"),
                            col("vehicle_id"),
                            col("experience_years")),256))
            .dropDuplicates(["driver_id","hash_key"])
        )
        driver_table="miniproject.silver.driver"
        is_empty=spark.table(driver_table).limit(1).count()==0
        if is_empty:
            driver_final.write.mode("append").saveAsTable(driver_table)
        else:
            driver_final.createOrReplaceTempView("driver_final")
            spark.sql("""
        MERGE INTO miniproject.silver.driver t
        USING driver_final s
        ON t.driver_id = s.driver_id

        WHEN MATCHED AND t.hash_key <> s.hash_key
        THEN UPDATE SET 
        driver_name = s.driver_name,
        vehicle_id = s.vehicle_id,
        experience_years = s.experience_years,
        hash_key = s.hash_key

        WHEN NOT MATCHED THEN INSERT (
        driver_id,
        driver_name,
        vehicle_id,
        experience_years,
        hash_key
        ) VALUES (
        s.driver_id,
        s.driver_name,
        s.vehicle_id,
        s.experience_years,
        s.hash_key
        )""")
    records_count=driver_final.count()
    spark.sql(f"""
    INSERT INTO miniproject.meta_table.audit_table
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'silver',
        'INCREMENTAL',
        current_timestamp(),
        current_timestamp(),
        {records_count},
        'SUCCESS',
        'none'
    )
    """)
except Exception as e:
    spark.sql(f"""
    INSERT INTO miniproject.meta_table.audit_table
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'silver',
        'INCREMENTAL',
        NULL,
        current_timestamp(),
        0,
        'FAILED',
        '{str(e).replace("'", " ")}'
    )
    """)
    raise


# COMMAND ----------

# DBTITLE 1,Cell 3
audit_table = "miniproject.meta_table.audit_table"
table_name = "route"
bronze_table = "miniproject.bronze.route"
try:
    last_load_ts = spark.sql(f"""
    SELECT MAX(last_load_timestamp)
    FROM {audit_table}
    WHERE table_name = '{table_name}'
    AND layer = 'silver'
    AND status = 'SUCCESS'
    """).collect()[0][0]
    
    if last_load_ts is None:
        bronze_df1 = spark.read.table(bronze_table)
    else:
        bronze_df1 = spark.read.table(bronze_table) \
            .filter(col("ingestion_time") > last_load_ts)
    bronze_count = bronze_df.count()

    if bronze_count == 0:
        print("No new data")

        spark.sql(f"""
        INSERT INTO {audit_table}
        (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
        VALUES (
            '{table_name}',
            'silver',
            'INCREMENTAL',
            current_timestamp(),
            current_timestamp(),
            0,
            'NO_DATA',
            'No new records'
        )
        """)

        pass
    else:
        route=bronze_df1.select("route_id", "source", "destination", "distance_km")
        route_clean=(route
                    .withColumn("source",initcap(col("source")))
                    .withColumn("destination",initcap(col("destination")))
                    .withColumn("distance_km",col("distance_km").cast("integer"))
                    .fillna({"distance_km": 0})
                    .filter(
                        col("route_id").isNotNull() & 
                        col("source").isNotNull() & 
                        col("destination").isNotNull() &
                        (col("distance_km")>0))            
        )
        route_final=(
            route_clean
            .withColumn(
                "hash_key",
                sha2(concat_ws("||",
                            col("source"),
                            col("destination"),
                            col("distance_km")),256))
            .dropDuplicates(["route_id","hash_key"])
        )
        route_table="miniproject.silver.route"
        is_empty=spark.table(route_table).limit(1).count()==0
        if is_empty:
            route_final.write.mode("append").saveAsTable(route_table)
        else:
            route_final.createOrReplaceTempView("route_final")
            spark.sql("""
        MERGE INTO miniproject.silver.route t
        USING route_final s
        ON t.route_id = s.route_id

        WHEN MATCHED AND t.hash_key <> s.hash_key
        THEN UPDATE SET 
        source = s.source,
        destination = s.destination,
        distance_km = s.distance_km,
        hash_key = s.hash_key

        WHEN NOT MATCHED THEN INSERT (
        route_id,
        source,
        destination,
        distance_km,
        hash_key
        ) VALUES (
        s.route_id,
        s.source,
        s.destination,
        s.distance_km,
        s.hash_key
        )"""
        )
    records_count=route_final.count()
    spark.sql(f"""
    INSERT INTO miniproject.meta_table.audit_table
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'silver',
        'INCREMENTAL',
        current_timestamp(),
        current_timestamp(),
        '{records_count}',
        'SUCCESS',
        'none'
    )
    """)
except Exception as e:
    spark.sql(f"""
    INSERT INTO miniproject.meta_table.audit_table
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'silver',
        'INCREMENTAL',      
        NULL,
        current_timestamp(),
        0,
        'FAILED',
        '{str(e).replace("'", " ")}'
    )
    """)
    raise


# COMMAND ----------

# DBTITLE 1,Cell 4
audit_table = "miniproject.meta_table.audit_table"
table_name = "vehicle"
bronze_table = "miniproject.bronze.vehicle"
try:
    last_load_ts = spark.sql(f"""
    SELECT MAX(last_load_timestamp)
    FROM {audit_table}
    WHERE table_name = '{table_name}'
    AND layer = 'silver'
    AND status = 'SUCCESS'
    """).collect()[0][0]
    
    if last_load_ts is None:
        bronze_df2 = spark.read.table(bronze_table)
    else:
        bronze_df2 = spark.read.table(bronze_table) \
            .filter(col("ingestion_time") > last_load_ts)
    if bronze_df2.limit(1).count() == 0:
        print("No new data for vehicle")
    else:
        vehicle=bronze_df2.select("vehicle_id", "vehicle_type", "capacity", "depot")
        vehicle_clean=(vehicle
                    .withColumn("vehicle_type",initcap(col("vehicle_type")))
                    .withColumn("depot",initcap(col("depot")))
                    .withColumn("capacity",col("capacity").cast("integer"))
                    .fillna({"capacity": 0})
                    .fillna({"depot": "Unknown"})
                    .filter(
                        col("vehicle_id").isNotNull() & 
                        (col("capacity")>=0 ))
        )
        vehicle_final=(
            vehicle_clean
            .withColumn(
                "hash_key",
                sha2(concat_ws("||",
                            col("vehicle_type"),
                            col("capacity"),
                            col("depot")),256))
            .dropDuplicates(["vehicle_id","hash_key"])
        )
        vehicle_table="miniproject.silver.vehicle"
        is_empty=spark.table(vehicle_table).limit(1).count()==0
        if is_empty:
            vehicle_final.write.mode("append").saveAsTable(vehicle_table)
        else:
            vehicle_final.createOrReplaceTempView("vehicle_final")
            spark.sql("""
        MERGE INTO miniproject.silver.vehicle t
        USING vehicle_final s
        ON t.vehicle_id = s.vehicle_id

        WHEN MATCHED AND t.hash_key <> s.hash_key
        THEN UPDATE SET 
        vehicle_type = s.vehicle_type,
        capacity = s.capacity,
        depot = s.depot,
        hash_key = s.hash_key

        WHEN NOT MATCHED THEN INSERT (
        vehicle_id,
        vehicle_type,
        capacity,
        depot,
        hash_key
        ) VALUES (
        s.vehicle_id,
        s.vehicle_type,
        s.capacity,
        s.depot,
        s.hash_key
        )"""
        )
    records_count=vehicle_final.count()
    spark.sql(f"""
    INSERT INTO miniproject.meta_table.audit_table
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'silver',
        'INCREMENTAL',
        current_timestamp(),
        current_timestamp(),
        '{records_count}',
        'SUCCESS',
        'none'
    )
    """)
except Exception as e:
    spark.sql(f"""
    INSERT INTO miniproject.meta_table.audit_table
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'silver',
        'INCREMENTAL',
        NULL,
        current_timestamp(),
        0,
        'FAILED',
        '{str(e).replace("'", " ")}'
    )
    """)
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC **TRIPS TABLE CLEANING**

# COMMAND ----------

# DBTITLE 1,Cell 6
audit_table = "miniproject.meta_table.audit_table"
table_name = "trip"
bronze_table = "miniproject.bronze.trip"
try:
    last_load_ts = spark.sql(f"""
    SELECT MAX(last_load_timestamp)
    FROM {audit_table}
    WHERE table_name = '{table_name}'
    AND layer = 'silver'
    AND status = 'SUCCESS'
    """).collect()[0][0]
    
    if last_load_ts is None:
        bronze_df3 = spark.read.table(bronze_table)
    else:
        bronze_df3 = spark.read.table(bronze_table) \
            .filter(col("ingestion_time") > last_load_ts)
    if bronze_df3.limit(1).count() == 0:
        print("No new data for trip")
    else:
        trip=bronze_df3.select("trip_id","driver_id", "vehicle_id", "route_id", "trip_date", "passengers", "revenue")
        trips_final = (trip
            .withColumn("trip_date", try_to_date(col("trip_date"), "dd-MM-yyyy"))
            .withColumn("passengers", expr("try_cast(passengers as int)"))
            .withColumn("revenue", expr("try_cast(revenue as int)"))
            .filter(
                col("trip_id").isNotNull() &
                col("driver_id").isNotNull() &
                col("vehicle_id").isNotNull() &
                col("route_id").isNotNull() &
                col("trip_date").isNotNull() &
                col("passengers").isNotNull() &
                (col("passengers") > 0) &
                (col("passengers") <= 100) &
                col("revenue").isNotNull() &
                (col("revenue") >= 0)
            )
            .withColumn(
                "hash_key",
                sha2(concat_ws("||",
                            col("trip_id"),
                            col("driver_id"),
                            col("vehicle_id"),
                            col("route_id"),
                            col("trip_date"),
                            col("passengers"),
                            col("revenue")),256)
            )
            .dropDuplicates(["trip_id","hash_key"])
        )
        trip_table="miniproject.silver.trip"
        is_empty=spark.table(trip_table).limit(1).count()==0
        if is_empty:
            trips_final.write.mode("append").saveAsTable(trip_table)
        else:
            trips_final.createOrReplaceTempView("trip_final")
        spark.sql("""
        MERGE INTO miniproject.silver.trip t
        USING trip_final s
        ON t.trip_id = s.trip_id

        WHEN MATCHED AND t.hash_key <> s.hash_key
        THEN UPDATE SET
        driver_id = s.driver_id,
        vehicle_id = s.vehicle_id,
        route_id = s.route_id,
        trip_date = s.trip_date,
        passengers = s.passengers,
        revenue = s.revenue,
        hash_key = s.hash_key

        WHEN NOT MATCHED THEN INSERT (
        trip_id,
        driver_id,
        vehicle_id,
        route_id,
        trip_date,
        passengers,
        revenue,
        hash_key
        ) VALUES (
        s.trip_id,
        s.driver_id,
        s.vehicle_id,
        s.route_id,
        s.trip_date,
        s.passengers,
        s.revenue,
        s.hash_key
        )"""
        )
    records_count=trips_final.count()
    spark.sql(f"""
    INSERT INTO miniproject.meta_table.audit_table
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'silver',
        'INCREMENTAL',
        current_timestamp(),
        current_timestamp(),
        '{records_count}',
        'SUCCESS',
        'none'
    )
    """)
except Exception as e:
    spark.sql(f"""
    INSERT INTO miniproject.meta_table.audit_table
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'silver',
        'INCREMENTAL',
        NULL,
        current_timestamp(),
        0,
        'FAILED',
        '{str(e).replace("'", " ")}'
    )
    """)
    raise


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from miniproject.silver.driver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from miniproject.silver.route

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from miniproject.silver.trip