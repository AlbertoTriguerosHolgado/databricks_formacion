# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: procesos
# MAGIC Lee `mercedes.bronze.datos`, filtra `PROCESS` y `TE1500` y guarda una fila limpia por proceso en `mercedes.silver.procesos`.
# MAGIC
# MAGIC **Requisitos:** notebook Python en Databricks con Unity Catalog, acceso a Bronze y permisos para crear/escribir en Silver y en el volumen de checkpoints. Los datos de prueba son los 100 procesos del fichero unificado.
# MAGIC
# MAGIC Las cinco reglas del Excel son: fecha `dd/MM/yyyy`, primera parte de `StudID`, `Plant` sin espacios, `Version` en mayúsculas y energía sin `J`. Las medidas con coma decimal se convierten además a números.

# COMMAND ----------

from pyspark.sql import functions as F

#Manual
BRONZE = "mercedes.bronze.datos"
SILVER = "mercedes.silver.procesos"
CHECKPOINT = "/Volumes/mercedes/silver/checkpoints/procesos"

#JOB
# tabla_origen = dbutils.widgets.get("tabla_origen")
# tabla_destino = dbutils.widgets.get("tabla_destino")
# checkpoint_path = dbutils.widgets.get("checkpoint_path")

# Date y Time del JSON representan la hora local del proceso.
spark.conf.set("spark.sql.session.timeZone", "Europe/Madrid")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparar Silver y el checkpoint
# MAGIC `checkpoints` es un volumen; `procesos` es la carpeta exclusiva de este flujo. Si el volumen ya existe, se reutiliza. Si no tienes permisos de creación, pide que lo prepare el administrador.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS mercedes.silver")
spark.sql("CREATE VOLUME IF NOT EXISTS mercedes.silver.checkpoints")
dbutils.fs.mkdirs(CHECKPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definir el esquema del body
# MAGIC `body` es un string con JSON. Solo necesitamos `MeasurementParameter`, una lista de objetos `unit`, `key` y `value`. `GraphicalData` no interviene en este ejercicio.

# COMMAND ----------

BODY_SCHEMA = """
STRUCT<MeasurementData: STRUCT<MeasurementParameter:
    ARRAY<STRUCT<unit: STRING, key: STRING, value: STRING>>
>>
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Convertir parámetros en columnas
# MAGIC Creamos un mapa `key → value` para extraer campos sin multiplicar las filas con `explode`. Las cadenas vacías pasan a `null`. Se conserva una fila por proceso.
# MAGIC
# MAGIC Un JSON mal formado o un parámetro con clave duplicada detiene el flujo en lugar de producir datos ambiguos.

# COMMAND ----------

def preparar_parametros(df):
    filtrado = df.filter(
        (F.col("transactiontype") == "PROCESS") &
        (F.col("subtype") == "TE1500")
    )
    return (
        filtrado
        .withColumn("json_body", F.from_json(
            "body", BODY_SCHEMA, {"mode": "FAILFAST"}
        ))
        .withColumn("parametros", F.map_from_entries(F.transform(
            F.col("json_body.MeasurementData.MeasurementParameter"),
            lambda p: F.struct(p["key"], p["value"])
        )))
    )


def parametro(key):
    valor = F.trim(F.col("parametros").getItem(key))
    return F.when(valor == "", F.lit(None).cast("string")).otherwise(valor)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Limpiar las cinco entradas del ejercicio
# MAGIC `ToolNo` e identificadores se conservan como texto. La energía será un entero en julios. La fecha será `DATE`; `Time` se conserva como string `HH:mm:ss`.
# MAGIC
# MAGIC `version_mensaje` viene de Bronze (`V8`); `version_proceso` viene del body (`TE1500AC`). Lo mismo ocurre con el tipo del mensaje y el tipo del proceso.

# COMMAND ----------

def transformar_procesos(df):
    df = preparar_parametros(df)

    columnas_base = [
        F.col("event_hub_enque_date_utc"),
        F.col("event_hub_enque_timestamp_utc"),
        F.col("msbtopic"),
        F.col("uniqueid"),
        F.col("type").alias("tipo_mensaje"),
        F.col("subtype"),
        F.col("transactiontype"),
        F.col("version").alias("version_mensaje"),
        F.col("delta_write_timestamp_utc").alias("bronze_write_timestamp_utc"),
        F.to_date(parametro("Date"), "dd/MM/yyyy").alias("fecha_proceso"),
        parametro("Time").alias("hora_proceso"),
        F.upper(parametro("Version")).alias("version_proceso"),
        F.split(parametro("StudID"), "_").getItem(0).alias("stud_id"),
        parametro("Plant").alias("plant"),
        F.split(parametro("WeldEnergy"), " ").getItem(0)
            .cast("int").alias("weld_energy_j"),
    ]

    # Campos de texto: preservar códigos e identificadores.
    textos = {
        "StudType": "stud_type", "CarbodyID": "carbody_id",
        "ProductionNumber": "production_number", "Location": "location",
        "Line": "line", "Building": "building", "Station": "station",
        "Robot": "robot", "Controller": "controller", "ToolNo": "tool_no",
        "Quality": "quality", "Type": "tipo_proceso",
        "FaultCode": "fault_code", "Remarks": "remarks",
    }

    # Números enteros: recuentos y tiempos en milisegundos.
    enteros = {
        "JoiningSpotSeries": "joining_spot_series",
        "TotalRecords": "total_records_origen",
        "WeldTimeActual": "weld_time_ms",
        "WeldToolSlideForwardMoveTime": "slide_forward_move_time_ms",
        "WeldToolSlideBackwardMoveTime": "slide_backward_move_time_ms",
        "FeedingTimeActual": "feeding_time_ms",
        "FeedingTimeStudInTube": "feeding_time_stud_in_tube_ms",
        "WeldToolMoveTimeLoadingPinBackward": "loading_pin_backward_time_ms",
        "WeldToolMoveTimeLoadingPinForward": "loading_pin_forward_time_ms",
    }

    # Medidas: sustituir coma decimal por punto y convertir a DOUBLE.
    medidas = {
        "PilotVoltageActual": "pilot_voltage_v",
        "WeldVoltageActual": "weld_voltage_v",
        "WeldCurrentActualPositive": "weld_current_a",
        "LMLiftHeightActual": "lift_height_mm",
        "LMPenetrationActual": "penetration_mm",
        "LMPositionActual": "actual_position_mm",
        "LMPositionReference": "reference_position_mm",
    }

    resultado = df.select(
        *columnas_base,
        *[parametro(key).alias(nombre) for key, nombre in textos.items()],
        *[parametro(key).cast("int").alias(nombre)
          for key, nombre in enteros.items()],
        *[F.regexp_replace(parametro(key), ",", ".").cast("double").alias(nombre)
          for key, nombre in medidas.items()],
    )

    # Timestamp local derivado para facilitar análisis posteriores.
    return resultado.withColumn(
        "fecha_hora_proceso",
        F.to_timestamp(
            F.concat_ws(" ", F.col("fecha_proceso").cast("string"),
                        F.col("hora_proceso")),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Revisar una muestra antes de escribir
# MAGIC Esta lectura es batch y solo sirve para comprobar las reglas. La escritura del siguiente paso utiliza `readStream`. No se realiza ninguna inserción en esta celda.

# COMMAND ----------

muestra = transformar_procesos(spark.table(BRONZE))
display(muestra.select(
    "stud_id", "plant", "version_mensaje", "version_proceso",
    "fecha_proceso", "hora_proceso", "weld_energy_j",
    "pilot_voltage_v", "penetration_mm"
).limit(10))

# Solo campos obligatorios del ejercicio; posiciones y observaciones pueden ser null.
obligatorios = ["stud_id", "plant", "version_proceso", "fecha_proceso", "weld_energy_j"]
condicion_nulos = F.lit(False)
for campo in obligatorios:
    condicion_nulos = condicion_nulos | F.col(campo).isNull()
assert muestra.filter(condicion_nulos).limit(1).count() == 0, "Revisar campos obligatorios"
print("Filas PROCESS / TE1500 disponibles en Bronze:", muestra.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Leer Bronze en streaming
# MAGIC La primera ejecución lee los datos existentes. En las siguientes, el checkpoint permite continuar desde los cambios pendientes. Bronze debe recibir nuevas filas mediante append; este ejercicio no procesa actualizaciones ni borrados.

# COMMAND ----------

bronze_stream = spark.readStream.table(BRONZE)
procesos_stream = transformar_procesos(bronze_stream)

print("¿Es streaming?", procesos_stream.isStreaming)
procesos_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Escribir en Silver con availableNow
# MAGIC El trigger procesa los datos pendientes al iniciar la ejecución y termina. `toTable` crea la tabla Delta si no existe. La tabla guarda registros limpios, sin agregarlos para el dashboard.
# MAGIC
# MAGIC Reutiliza el mismo checkpoint y destino en cada ejecución. No borres el checkpoint para volver a ejecutar: perderías el progreso y podrías duplicar datos. Tampoco lo compartas con el flujo de mensajes.

# COMMAND ----------

query = (
    procesos_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .queryName("bronze_a_silver_procesos")
    .toTable(SILVER)
)

query.awaitTermination()
print("Carga finalizada:", SILVER)
print("Último progreso:", query.lastProgress)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notas para el dashboard
# MAGIC - `total_records_origen` es el valor recibido en el JSON; el total real de Silver se calcula con `COUNT(*)`, sin sumar ese campo.
# MAGIC - Las unidades están en los nombres: `_v`, `_a`, `_j`, `_mm` y `_ms`. Los campos sin valor se guardan como `null`.
# MAGIC - Las correspondencias de `Quality`, polaridad positiva de corriente y posición de elevación siguen las propuestas del Excel; deben validarse con el sistema real.
# MAGIC - El notebook usa transformaciones nativas de Spark y no necesita pandas ni librerías Excel en Databricks.
# MAGIC
# MAGIC Documentación: [Delta streaming](https://docs.databricks.com/aws/en/structured-streaming/delta-lake) y [availableNow](https://docs.databricks.com/aws/en/structured-streaming/tutorial).
# MAGIC
# MAGIC El notebook está preparado para importarse en Databricks. Su ejecución requiere tu entorno, tablas y permisos.