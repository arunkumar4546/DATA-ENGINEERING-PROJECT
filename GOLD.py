# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import date

# COMMAND ----------

# DBTITLE 1,Cell 2
audit_table = "miniproject.meta_table.audit_table"
table_name = "dim_driver"
silver_table = "miniproject.silver.driver"
gold_table = "miniproject.gold.dim_driver"

try:
    driver=spark.table("miniproject.silver.driver")
    driver_table = "miniproject.gold.dim_driver"

    run_date = date.today()  # Freeze the date for this run
    run_date_str = run_date.strftime("%Y-%m-%d")

    # Check if table is empty
    is_empty = spark.table(driver_table).limit(1).count() == 0

    if is_empty:
        # Initial Load
        driver_last=(driver
            .withColumn("effective_start_date", lit(run_date))
            .withColumn("effective_end_date", lit(None).cast("date"))
            .withColumn("is_active", lit(True)))
        driver_last.write.format("delta") \
            .mode("append") \
            .saveAsTable(driver_table)

    else:
        # Incremental Load (SCD Type 2)
        driver.createOrReplaceTempView("driver_final")

        spark.sql(f"""
            MERGE INTO {driver_table} t
            USING driver_final s
            ON t.driver_id = s.driver_id AND t.is_active = true
            
            -- Close old record if data changed
            WHEN MATCHED AND t.hash_key != s.hash_key
            THEN UPDATE SET
                t.is_active = false,
                t.effective_end_date = DATE('{run_date_str}')
            
            -- Insert new records
            WHEN NOT MATCHED
            THEN INSERT (
                driver_id ,
                driver_name ,
                vehicle_id ,
                experience_years ,
                hash_key ,
                is_active ,
                effective_start_date ,
                effective_end_date 
            )
            VALUES (
                s.driver_id ,
                s.driver_name ,
                s.vehicle_id ,
                s.experience_years ,
                s.hash_key ,
                true ,
                DATE('{run_date_str}') ,
                NULL
            )
        """)
    record_count = driver.count()
    spark.sql(f"""
            INSERT INTO {audit_table}
            VALUES (
            '{table_name}',
            'gold',
            'SCD2',
            current_timestamp(),
            current_timestamp(),
            {record_count},
            'SUCCESS',
            NULL
        )
        """)
except Exception as e:
    print(e)
    spark.sql(f"""
        INSERT INTO {audit_table}
        VALUES (
        '{table_name}',
        'gold',
        'SCD2',
        current_timestamp(),
        current_timestamp(),
        NULL,
        'FAILURE',
        '{e}'
    )
    """)


# COMMAND ----------

# DBTITLE 1,Cell 3
audit_table = "miniproject.meta_table.audit_table"
table_name = "dim_route"
silver_table = "miniproject.silver.route"
gold_table = "miniproject.gold.dim_route"
try:
    route=spark.table("miniproject.silver.route")
    route_table = "miniproject.gold.dim_route"

    run_date = date.today()  # Freeze the date for this run
    run_date_str = run_date.strftime("%Y-%m-%d")
    # Check if table is empty
    is_empty = spark.table(route_table).limit(1).count() == 0

    if is_empty:
        # Initial Load
        route_last=(route
            .withColumn("effective_start_date", lit(run_date))
            .withColumn("effective_end_date", lit(None).cast("date"))
            .withColumn("is_active", lit(True)))

        route_last.write.format("delta") \
            .mode("append") \
            .saveAsTable(route_table)

    else:
        # Incremental Load (SCD Type 2)
        route.createOrReplaceTempView("route_final")

        spark.sql(f"""
            MERGE INTO {route_table} t
            USING route_final s
            ON t.route_id = s.route_id AND t.is_active = true
            
            -- Close old record if data changed
            WHEN MATCHED AND t.hash_key != s.hash_key
            THEN UPDATE SET
                t.is_active = false,
                t.effective_end_date = DATE('{run_date_str}')
            
            -- Insert new records
            WHEN NOT MATCHED
            THEN INSERT (
            route_id ,
            source ,
            destination ,
            distance_km ,
            hash_key ,
            is_active ,
            effective_start_date ,
            effective_end_date 
            )
            VALUES (
            s.route_id ,
            s.source ,
            s.destination ,
            s.distance_km ,
            s.hash_key ,
            true ,
            DATE('{run_date_str}') ,
            NULL
            )
        """)
    record_count = route.count()
    spark.sql(f"""
                INSERT INTO {audit_table}
                VALUES (
                '{table_name}',
                'gold',
                'SCD2',
                current_timestamp(),
                current_timestamp(),
                {record_count},
                'SUCCESS',
                NULL
            )
            """)
except Exception as e:
    print(f"Error: {e}")
    spark.sql(f"""
        INSERT INTO {audit_table}
        VALUES (
        '{table_name}',
        'gold',
        'SCD2',
        current_timestamp(),
        current_timestamp(),
        0,
        'FAILURE',
        '{e}'
    )
    """)


# COMMAND ----------

# DBTITLE 1,Cell 4
audit_table = "miniproject.meta_table.audit_table"
table_name = "dim_vehicle"
silver_table = "miniproject.silver.vehicle"
gold_table = "miniproject.gold.dim_vehicle"
try:
    vehicle=spark.table("miniproject.silver.vehicle")
    vehicle_table = "miniproject.gold.dim_vehicle"

    run_date = date.today()  # Freeze the date for this run
    run_date_str = run_date.strftime("%Y-%m-%d")
    # Check if table is empty
    is_empty = spark.table(vehicle_table).limit(1).count() == 0

    if is_empty:
        # Initial Load
        vehicle_last=(vehicle
            .withColumn("effective_start_date", lit(run_date))
            .withColumn("effective_end_date", lit(None).cast("date"))
            .withColumn("is_active", lit(True)))
        vehicle_last.write.format("delta") \
            .mode("append") \
            .saveAsTable(vehicle_table)

    else:
        # Incremental Load (SCD Type 2)
        vehicle.createOrReplaceTempView("vehicle_final")

        spark.sql(f"""
            MERGE INTO {vehicle_table} t
            USING vehicle_final s
            ON t.vehicle_id = s.vehicle_id AND t.is_active = true
            
            -- Close old record if data changed
            WHEN MATCHED AND t.hash_key != s.hash_key
            THEN UPDATE SET
                t.is_active = false,
                t.effective_end_date = DATE('{run_date_str}')
            
            -- Insert new records
            WHEN NOT MATCHED
            THEN INSERT (
                vehicle_id ,
                vehicle_type ,
                capacity ,
                depot ,
                hash_key ,
                is_active ,
                effective_start_date ,
                effective_end_date 
            )
            VALUES (
                s.vehicle_id ,
                s.vehicle_type ,
                s.capacity ,
                s.depot ,
                s.hash_key ,
                true ,
                DATE('{run_date_str}') ,
                NULL
            )
        """)
    record_count = vehicle.count()
    spark.sql(f"""
                    INSERT INTO {audit_table}
                    VALUES (
                    '{table_name}',
                    'gold',
                    'SCD2',
                    current_timestamp(),
                    current_timestamp(),
                    {record_count},
                    'SUCCESS',
                    NULL
                )
                """)
except Exception as e:
    print(f"Error: {e}")
    spark.sql(f"""
        INSERT INTO {audit_table}
        VALUES (
        '{table_name}',
        'gold',
        'SCD2',
        current_timestamp(),
        current_timestamp(),
        0,
        'FAILURE',
        '{e}'
    )
    """)


# COMMAND ----------

# DBTITLE 1,Fact Trip Insert (fixed)
audit_table = "miniproject.meta_table.audit_table"
table_name = "fact_trip"
gold_table = "miniproject.gold.fact_trip"
try:
    spark.sql("""
    INSERT INTO miniproject.gold.fact_trip
    SELECT
        d.driver_sk,
        v.vehicle_sk,
        r.route_sk,
        t.trip_id,
        t.trip_date,
        t.passengers,
        t.revenue
    FROM miniproject.silver.trip t
    JOIN miniproject.gold.dim_driver d
        ON t.driver_id = d.driver_id
    AND d.is_active = true
    JOIN miniproject.gold.dim_vehicle v
        ON t.vehicle_id = v.vehicle_id
    AND v.is_active = true
    JOIN miniproject.gold.dim_route r
        ON t.route_id = r.route_id
    AND r.is_active = true;
    """)
    record_count = spark.table("miniproject.silver.trip").count()
    spark.sql(f"""
        INSERT INTO {audit_table}
        VALUES (
            '{table_name}',
            'gold',
            'INCREMENTAL',
            current_timestamp(),
            current_timestamp(),
            {record_count},
            'SUCCESS',
            NULL
        )
        """)

except Exception as e:

    # FAILED audit
    spark.sql(f"""
    INSERT INTO {audit_table}
    VALUES (
        '{table_name}',
        'gold',
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
# MAGIC select * from miniproject.gold.fact_trip order by trip_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from miniproject.gold.dim_driver