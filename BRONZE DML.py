# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Cell 2
table_name = "driver"
bronze_table = "miniproject.bronze.driver"
audit_table = "miniproject.meta_table.audit_table"
try:
    df = spark.read.csv("/Volumes/miniproject/bronze/datas/drivers.csv", header=True)
    df = df.withColumn("ingestion_date", current_date())
    df = df.withColumn("ingestion_time", current_timestamp())
    display(df)
    records_count = df.count()
    df.write.format("delta").mode("append").saveAsTable("miniproject.bronze.driver")
    spark.sql(f"""
    INSERT INTO {audit_table}
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'bronze',
        'APPEND',
        current_timestamp(),
        current_timestamp(),
        {records_count},
        'SUCCESS',
        'none'
    )
    """)
except Exception as e:
    print(e)
    spark.sql(f"""
    INSERT INTO {audit_table}
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'bronze',
        'APPEND',
        current_timestamp(),
        current_timestamp(),
        0,
        'FAILED',
        '{e}'
    )
    """)
    raise e


# COMMAND ----------

# DBTITLE 1,Cell 3
table_name = "vehicle"
bronze_table = "miniproject.bronze.vehicle"
audit_table = "miniproject.meta_table.audit_table"
try:
    df1=spark.read.csv("/Volumes/miniproject/bronze/datas/vehicle.csv",header=True)
    df1=df1.withColumn("ingestion_date", current_date())
    df1= df1.withColumn("ingestion_time", current_timestamp())
    display(df1)
    records_count = df1.count()
    df1.write.format("delta").mode("append").saveAsTable("miniproject.bronze.vehicle")
    spark.sql(f"""
    INSERT INTO {audit_table}
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'bronze',
        'APPEND',
        current_timestamp(),
        current_timestamp(),
        {records_count},
        'SUCCESS',
        'none'
    )
    """)
except Exception as e:
    print(e)
    spark.sql(f"""
    INSERT INTO {audit_table}
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'bronze',
        'APPEND',
        current_timestamp(),
        current_timestamp(),
        0,
        'FAILED',
        '{e}'
    )
    """)

# COMMAND ----------

# DBTITLE 1,Cell 4
table_name = "route"
bronze_table = "miniproject.bronze.route"
audit_table = "miniproject.meta_table.audit_table"
try:
    df2=spark.read.csv("/Volumes/miniproject/bronze/datas/route.csv",header=True)
    df2=df2.withColumn("ingestion_date", current_date())
    df2= df2.withColumn("ingestion_time", current_timestamp())
    display(df2)
    records_count = df2.count()
    df2.write.format("delta").mode("append").saveAsTable("miniproject.bronze.route")
    spark.sql(f"""
    INSERT INTO {audit_table}
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'bronze',
        'APPEND',
        current_timestamp(),
        current_timestamp(),
        {records_count},
        'SUCCESS',
        'none'
    )
    """)
except Exception as e:
    print(e)
    spark.sql(f"""
    INSERT INTO {audit_table}
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'bronze',
        'APPEND',
        current_timestamp(),
        current_timestamp(),
        0,
        'FAILED',
        '{e}'
    )
    """)

# COMMAND ----------

# DBTITLE 1,Cell 5
table_name = "trip"
bronze_table = "miniproject.bronze.trip"
audit_table = "miniproject.meta_table.audit_table"
try:
    df3=spark.read.csv("/Volumes/miniproject/bronze/datas/trips.csv",header=True)
    df3=df3.withColumn("ingestion_date", current_date())
    df3=df3.withColumn("ingestion_time", current_timestamp())
    display(df3)
    records_count = df3.count()
    df3.write.format("delta").mode("append").saveAsTable("miniproject.bronze.trip")
    spark.sql(f"""
    INSERT INTO {audit_table}
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'bronze',
        'APPEND',
        current_timestamp(),
        current_timestamp(),
        {records_count},
        'SUCCESS',
        'none'
    )
    """)
except Exception as e:
    print(e)
    spark.sql(f"""
    INSERT INTO {audit_table}
    (table_name, layer, load_type, last_load_timestamp, last_run_timestamp, record_count, status, error_message)
    VALUES (
        '{table_name}',
        'bronze',
        'APPEND',
        current_timestamp(),
        current_timestamp(),
        0,
        'FAILED',
        '{e}'
    )
    """)
