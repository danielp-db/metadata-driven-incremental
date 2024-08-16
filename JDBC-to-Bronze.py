# Databricks notebook source
# MAGIC %md # PARAMETERS

# COMMAND ----------

import json

dbutils.widgets.text("processes", "")

processes = dbutils.widgets.get("processes")

# COMMAND ----------

processes = [
  {
    "source_table": "dbo.dp_gia_demo",
    "partition_column": "val",
    "filter_column": "dt",
    #"filter_condition": "WHERE dt < '2024-01-02'",
    "target_table": "daniel_perez.gb_audit.gia_demo_landing"
  }
]

# COMMAND ----------

# MAGIC %md # CONSTANTS

# COMMAND ----------

URL = f"jdbc:sqlserver://oneenvsql.database.windows.net:1433;database=oneenvsqldb"
DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

USER=dbutils.secrets.get(scope = "oneenvkeys", key = "dbuser")
PASSWORD=dbutils.secrets.get(scope = "oneenvkeys", key = "dbpassword")

# COMMAND ----------

# MAGIC %md # FUNCTIONS

# COMMAND ----------

def get_filter_condition(target_table, filter_column):
  if spark.catalog.tableExists(target_table):
    max_filter_column = spark.sql(f"SELECT MAX({filter_column}) FROM {target_table}").first()[0]
  else:
    max_filter_column = None
  
  if max_filter_column is not None:
    filter_condition = f"WHERE {filter_column} > '{max_filter_column}'"
  else:
    filter_condition = ""
  
  return filter_condition

# COMMAND ----------

def jdbc_query(query,
               partition_column=None,
               lower_bound=None, upper_bound=None,
               num_partitions=10):

  df = (
    spark.read.format("jdbc")
    .option("url", URL)
    .option("dbtable", f"({query}) as subq")
    #.option("query", query)
    .option("user", USER)
    .option("password", PASSWORD)
    .option("driver", DRIVER)
  )
    
  if partition_column is not None:
    df = (
      df
      .option("partitionColumn", partition_column)
      .option("lowerBound", lower_bound)
      .option("upperBound", upper_bound)
      .option("numPartitions", num_partitions)
    )
  
  return df.load()

# COMMAND ----------

def get_partition_bounds(source_table, filter_condition, partition_column):
  query = f"""
    SELECT
      min({partition_column}) lower_bound,
      max({partition_column}) upper_bound
    FROM {source_table} {filter_condition}"""
  
  df = jdbc_query(query)
  
  result = df.first() 
  lower_bound, upper_bound = result["lower_bound"], result["upper_bound"]
  
  return lower_bound, upper_bound

# COMMAND ----------

# MAGIC %md # MAIN

# COMMAND ----------

for process in processes:
  source_table = process.get("source_table")
  target_table = process.get("target_table")
  partition_column = process.get("partition_column")
  filter_condition = process.get("filter_condition")
  filter_column = process.get("filter_column")

  print(f"Ingesting {process}")


  if filter_condition is None:
    filter_condition = get_filter_condition(target_table, filter_column)

  lower_bound, upper_bound = get_partition_bounds(source_table, filter_condition, partition_column)

  query = f"SELECT * FROM {source_table} {filter_condition}"

  if partition_column is None or lower_bound is None or upper_bound is None:
    df = jdbc_query(query)
  else:
    df = jdbc_query(query, partition_column, lower_bound, upper_bound)

  if df.count() > 0:
    df.write.mode("append").saveAsTable(target_table)

  # TODO UPDATE CONTROL TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC --DESCRIBE HISTORY daniel_perez.gb_audit.gia_demo_landing;
# MAGIC SELECT * FROM daniel_perez.gb_audit.gia_demo_landing ORDER BY id, dt;
# MAGIC --DROP TABLE daniel_perez.gb_audit.gia_demo_landing;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daniel_perez.gb_audit.gia_demo_curated;
