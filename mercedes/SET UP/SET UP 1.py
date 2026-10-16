# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG mercedes;
# MAGIC CREATE SCHEMA mercedes.bronze;
# MAGIC CREATE SCHEMA mercedes.silver;
# MAGIC CREATE SCHEMA mercedes.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME mercedes.bronze.data;
# MAGIC CREATE VOLUME mercedes.silver.checkpoints;

# COMMAND ----------

# MAGIC %md
# MAGIC CREAMOS LA TABLA DE BRONZE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS mercedes.bronze.datos (
# MAGIC     event_hub_enque_date_utc DATE,
# MAGIC     event_hub_enque_timestamp_utc TIMESTAMP,
# MAGIC     msbtopic STRING,
# MAGIC     uniqueid STRING,
# MAGIC     type STRING,
# MAGIC     subtype STRING,
# MAGIC     transactiontype STRING,
# MAGIC     version STRING,
# MAGIC     body STRING,
# MAGIC     contamination_checks STRING,
# MAGIC     delta_write_timestamp_utc TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC