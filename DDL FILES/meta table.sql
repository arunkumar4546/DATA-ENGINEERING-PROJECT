-- Databricks notebook source
from pyspark.sql.functions import *

-- COMMAND ----------

create table miniproject.meta_table.audit_table (
  table_name string,
  layer string,
  load_type string,
  last_load_timestamp timestamp,
  last_run_timestamp timestamp,
  record_count bigint,
  status string,
  error_message string
)
using delta;

-- COMMAND ----------

select * from miniproject.meta_table.audit_table order by last_run_timestamp desc