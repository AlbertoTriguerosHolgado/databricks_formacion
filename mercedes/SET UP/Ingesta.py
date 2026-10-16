# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

ruta = "/Volumes/mercedes/bronze/data/Mensajes_y_Eventos_ETL_200_filas.xlsx"
tabla = "mercedes.bronze.datos"

spark.conf.set("spark.sql.session.timeZone", "UTC")

pdf = pd.read_excel(
    ruta,
    sheet_name="in",
    engine="openpyxl",
    dtype=str,
    keep_default_na=False
)

columnas = spark.table(tabla).columns

if set(pdf.columns) != set(columnas):
    raise ValueError("Las columnas del Excel no coinciden con la tabla")

pdf = pdf[columnas]

# Aceptar fechas con y sin fracciones de segundo.
pdf["event_hub_enque_date_utc"] = pd.to_datetime(
    pdf["event_hub_enque_date_utc"],
    format="mixed",
    errors="raise"
).dt.strftime("%Y-%m-%d")

timestamps = [
    "event_hub_enque_timestamp_utc",
    "delta_write_timestamp_utc"
]

for columna in timestamps:
    pdf[columna] = pd.to_datetime(
        pdf[columna],
        format="mixed",
        utc=True,
        errors="raise"
    ).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

schema = StructType([
    StructField(nombre, StringType(), True)
    for nombre in columnas
])

df = spark.createDataFrame(
    list(pdf.itertuples(index=False, name=None)),
    schema=schema
)

df = df.withColumn(
    "event_hub_enque_date_utc",
    F.to_date("event_hub_enque_date_utc", "yyyy-MM-dd")
)

for columna in timestamps:
    df = df.withColumn(
        columna,
        F.to_timestamp(
            F.col(columna),
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
        )
    )

display(df.limit(5))
print(f"Filas preparadas: {df.count()}")

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(tabla)