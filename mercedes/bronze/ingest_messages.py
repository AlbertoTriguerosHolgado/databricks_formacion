# Databricks notebook source
from pyspark.sql import functions as F

path = "/Volumes/mercedes/bronze/mensajes/*.json" 

df = (
    spark.read
        .format("text")
        .option("wholetext", "true")
        .load(path)
)

df_test = df.select(
    F.current_date().cast("string").alias("ingest_date"),
    F.date_format(
        F.current_timestamp(),
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    ).alias("timestamputc"),

    F.lit("msb/test/topic").alias("msbtopic"),
    F.lit("TEST_DEVICE").alias("type"),
    F.lit("TEST_SUBTYPE").alias("subtype"),
    F.lit("TEST_TRANSACTION").alias("transactiontype"),

    # Un ID diferente para cada mensaje
    F.expr("uuid()").alias("uniqueid"),

    F.lit("1.0").alias("version"),

    # Contenido completo del fichero JSON como STRING
    F.col("value").alias("body")
)

# COMMAND ----------

df_test.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("mercedes.bronze.mensajes")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `mercedes`; select * from `bronze`.`mensajes` limit 100;