# Databricks notebook source
import dlt
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

# MAGIC %md # CATALOG / SCHEMA

# COMMAND ----------

catalog = "daniel_perez"
bronze_schema = "gb_audit"

# COMMAND ----------

# MAGIC %md # TRANSFORMACIONES

# COMMAND ----------

def route(dataframe):
    return dataframe
 

# COMMAND ----------

def invoice_header(dataframe):
    return dataframe

# COMMAND ----------

# MAGIC %md # PROCESOS

# COMMAND ----------

processes = [
  {
    "process_type": "multiple",
    "target_table": "mulitple_incremental_tipo1",
    "source_tables": [{
                "table": f"{catalog}.{bronze_schema}.tabla1",
                "transformations": [invoice_header]
              },
              {
                "table": f"{catalog}.{bronze_schema}.tabla2",
                "transformations": [invoice_header]
              }],
    #"transformations": [invoice_header]
    "keys": ["id"],
    "sequence_by": "update_dt",
    "stored_as_scd_type": "1",
    "is_incremental": True
  },
  {
    "process_type": "multiple",
    "target_table": "mulitple_incremental_tipo2",
    "source_tables": [{
                "table": f"{catalog}.{bronze_schema}.tabla1",
                "transformations": [route]
              },
              {
                "table": f"{catalog}.{bronze_schema}.tabla2",
                "transformations": [route]
              }],
    #"transformations": [transformation]
    "keys": ["id"],
    "sequence_by": "update_dt",
    "stored_as_scd_type": "2",
    "is_incremental": True
  },
  {
    "process_type": "multiple",
    "target_table": "multiple_append",
    "source_tables": [{
                "table": f"{catalog}.{bronze_schema}.tabla1",
                "transformations": [route]
              },
              {
                "table": f"{catalog}.{bronze_schema}.tabla2",
                "transformations": [route]
              }],
  },
  {
    "process_type": "single",
    "target_table": "single_append",
    "source_table": f"{catalog}.{bronze_schema}.tabla1",
    #"transformations": [sales_center],
  }, 
  {
    "process_type": "single",
    "target_table": "single_incremental_tipo2",
    "source_table": f"{catalog}.{bronze_schema}.tabla1",
    #"transformations": [sales_center],
    "keys": ["id"],
    "sequence_by": "update_dt",
    "stored_as_scd_type": "2",
    "is_incremental": True
  },
    {
    "process_type": "single",
    "target_table": "single_incremental_tipo1",
    "source_table": f"{catalog}.{bronze_schema}.tabla1",
    #"transformations": [sales_center],
    "keys": ["id"],
    "sequence_by": "update_dt",
    "stored_as_scd_type": "1",
    "is_incremental": True
  }
]

# COMMAND ----------

# MAGIC %md # FRAMEWORK

# COMMAND ----------

def append_flow(source_table, intermediate_table):
  table = source_table.get("table")
  transformations = source_table.get("transformations")

  table_name = table.split(".")[-1]
  view_name = f"{table_name}_2_{intermediate_table}_view" #YOU HAVE TO CREATE AN INTERMEDIATE VIEW

  @dlt.view(name=view_name)
  def source():
    df = spark.readStream.table(table)

    if transformations and len(transformations) > 0:
      for transformation in transformations:
        df = transformation(df)
    return df

  @dlt.append_flow(target = intermediate_table, name = f"{view_name}_flow")
  def flow():
    return spark.readStream.table(f"live.{view_name}")

def multiple_process(process):
  target_table = process.get("target_table")
  source_tables = process.get("source_tables")
  keys = process.get("keys")
  sequence_by = process.get("sequence_by")
  stored_as_scd_type = process.get("stored_as_scd_type")
  is_incremental = process.get("is_incremental")
  transformations =  process.get("sales_center")

  
  union_table = f"_append_{target_table}" if is_incremental else target_table
  dlt.create_streaming_table(union_table) #INTERMEDIATE APPEND ONLY TABLE
  for source_table in source_tables:
    append_flow(source_table, union_table)

  #TRANSFORMATIONS AFTER THE FLOW
  #if len(transformation) > 0:
  #  intermediate_transformed_view = f"_append_{target_table}_view"
  #  @dlt.view(name=intermediate_transformed_view):
  #    df = spark.readStream.table(f"live.{intermediate_table}")
  #  
  #    for transformation in transformations:
  #      df = transformation(df)
  #    return df

  if is_incremental:
    dlt.create_streaming_table(target_table) #SCD TABLE
    dlt.apply_changes(
      target = target_table,
      source = union_table,
      keys = keys,
      sequence_by = sequence_by,
      stored_as_scd_type = stored_as_scd_type
    )

def single_process(process):
  target_table = process.get("target_table")
  source_table = process.get("source_table")
  keys = process.get("keys")
  sequence_by = process.get("sequence_by")
  stored_as_scd_type = process.get("stored_as_scd_type")
  is_incremental = process.get("is_incremental")
  transformations =  process.get("sales_center")

  source_view = source_table.split(".")[-1]
  
  @dlt.view(name=f"{source_view}_2_{target_table}")
  def bronze_view():
    df = spark.readStream.table(source_table)
    
    if transformations and len(transformations) > 0:
      for transformation in transformations:
        df = transformation(df)
    return df

  if is_incremental:
    dlt.create_streaming_table(target_table)
    dlt.apply_changes(
      target = target_table,
      source = f"{source_view}_2_{target_table}",
      keys = keys,
      sequence_by = sequence_by,
      stored_as_scd_type = stored_as_scd_type
    )
  else:
    @dlt.table(name=target_table)
    def silver_table():
      return spark.readStream.table(f"live.{source_view}_2_{target_table}")

# COMMAND ----------

for process in processes:
  process_type = process.get("process_type")

  if process_type == 'multiple':
    multiple_process(process)
  elif process_type == 'single':
    single_process(process)
