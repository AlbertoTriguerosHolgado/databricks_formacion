# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: mensajes
# MAGIC Este notebook limpia los mensajes de `mercedes.bronze.datos` y escribe una fila por mensaje en `mercedes.silver.mensajes`.
# MAGIC
# MAGIC Se filtra por `transactiontype = MESSAGES` y `subtype = TE1500AC`. No se calculan agrupaciones, rankings ni Top 10: se harán después en una vista materializada.
# MAGIC
# MAGIC **Requisitos:** notebook Python en Databricks con Unity Catalog y permisos de lectura en Bronze, escritura en Silver y acceso al volumen de checkpoints. Las reglas proceden de `KPIs_dashboard_errores(1).xlsx`.

# COMMAND ----------

from pyspark.sql import functions as F

BRONZE = "mercedes.bronze.datos"
SILVER = "mercedes.silver.mensajes"
CHECKPOINT = "/Volumes/mercedes/silver/checkpoints/mensajes"

#JOB
# tabla_origen = dbutils.widgets.get("tabla_origen")
# tabla_destino = dbutils.widgets.get("tabla_destino")
# checkpoint_path = dbutils.widgets.get("checkpoint_path")

# El offset del JSON identifica el instante; la sesión define su visualización.
spark.conf.set("spark.sql.session.timeZone", "Europe/Madrid")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparar el destino y el checkpoint
# MAGIC El volumen `checkpoints` contiene una carpeta exclusiva para mensajes. Se reutiliza en cada ejecución y es independiente de `procesos`. Si no puedes crear el esquema o el volumen, pide al administrador que los prepare.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS mercedes.silver")
spark.sql("CREATE VOLUME IF NOT EXISTS mercedes.silver.checkpoints")
dbutils.fs.mkdirs(CHECKPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definir el esquema del JSON
# MAGIC `body` contiene el objeto `Messages`. Declaramos los campos para extraerlos con `from_json`. `InProcess` llega como string y se convertirá a booleano; los identificadores seguirán siendo texto.

# COMMAND ----------

BODY_SCHEMA = """
STRUCT<Messages: STRUCT<
    Timestamp: STRING,
    DeviceName: STRING,
    ToolNo: STRING,
    StudId: STRING,
    Message_Id: STRING,
    Message_Text: STRUCT<`DE-DE`: STRING, `EN-US`: STRING>,
    InProcess: STRING,
    Component: STRING,
    Duration: STRING,
    OperationMode: STRING
>>
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aplicar las transformaciones
# MAGIC Las cinco entradas del ejercicio se limpian así:
# MAGIC - `Timestamp`: interpretar `dd/MM/yyyy HH:mm:ss Z`, conservando el offset.
# MAGIC - `DeviceName`: pasar a mayúsculas y crear una etiqueta con separadores ` / `.
# MAGIC - `ToolNo`: eliminar espacios iniciales y finales.
# MAGIC - `StudId`: conservar el primer segmento separado por `_`.
# MAGIC - `InProcess`: convertir `"true"` / `"false"` a booleano.
# MAGIC
# MAGIC También extraemos el texto EN-US, construimos `tool` y conservamos los tres campos de relleno. Las cadenas vacías pasan a `null`; no se eliminan registros. Un JSON mal formado detiene el flujo.

# COMMAND ----------

def texto_limpio(campo):
    valor = F.trim(F.col(campo))
    return F.when(valor == "", F.lit(None).cast("string")).otherwise(valor)


def transformar_mensajes(df):
    filtrado = df.filter(
        (F.col("transactiontype") == "MESSAGES") &
        (F.col("subtype") == "TE1500AC")
    )

    parseado = filtrado.withColumn(
        "json_body",
        F.from_json("body", BODY_SCHEMA, {"mode": "FAILFAST"})
    )

    limpio = parseado.select(
        # Metadatos de origen para trazabilidad.
        "event_hub_enque_date_utc",
        "event_hub_enque_timestamp_utc",
        "msbtopic",
        "uniqueid",
        F.col("type").alias("tipo_mensaje"),
        "subtype",
        "transactiontype",
        F.col("version").alias("version_mensaje"),
        "contamination_checks",
        F.col("delta_write_timestamp_utc").alias("bronze_write_timestamp_utc"),

        # Campos del JSON limpios y tipados.
        F.to_timestamp(
            texto_limpio("json_body.Messages.Timestamp"),
            "dd/MM/yyyy HH:mm:ss Z"
        ).alias("timestamp_mensaje"),
        F.upper(texto_limpio("json_body.Messages.DeviceName")).alias("device_name"),
        texto_limpio("json_body.Messages.ToolNo").alias("tool_no"),
        F.split(texto_limpio("json_body.Messages.StudId"), "_")
            .getItem(0).alias("stud_id"),
        texto_limpio("json_body.Messages.InProcess").cast("boolean").alias("in_process"),
        texto_limpio("json_body.Messages.Message_Id").alias("message_id"),
        texto_limpio("json_body.Messages.Message_Text.`EN-US`").alias("message_text_en"),
        texto_limpio("json_body.Messages.Message_Text.`DE-DE`").alias("message_text_de"),

        # Tres campos de relleno del modelo.
        texto_limpio("json_body.Messages.Component").alias("component"),
        texto_limpio("json_body.Messages.Duration").alias("duration"),
        texto_limpio("json_body.Messages.OperationMode").alias("operation_mode"),
    )

    limpio = limpio.withColumn(
        "device_label", F.regexp_replace("device_name", "_", " / ")
    )

    # Si falta equipo o herramienta, no construir una etiqueta incompleta.
    return limpio.withColumn(
        "tool",
        F.when(
            F.col("device_label").isNotNull() & F.col("tool_no").isNotNull(),
            F.concat_ws(" / ", F.col("device_label"), F.col("tool_no"))
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Revisar una muestra sin escribir
# MAGIC Esta lectura batch permite comprobar las reglas antes de lanzar el stream. El fichero de formación contiene 100 mensajes. Se comprueban los campos necesarios para el futuro dashboard; `false` en `in_process` es un valor válido.

# COMMAND ----------

muestra = transformar_mensajes(spark.table(BRONZE))

display(muestra.select(
    "timestamp_mensaje", "device_name", "tool_no", "tool",
    "stud_id", "in_process", "message_id", "message_text_en"
).limit(10))

obligatorios = [
    "timestamp_mensaje", "tool", "stud_id",
    "in_process", "message_id", "message_text_en"
]
condicion_nulos = F.lit(False)
for campo in obligatorios:
    condicion_nulos = condicion_nulos | F.col(campo).isNull()

assert muestra.filter(condicion_nulos).limit(1).count() == 0, "Revisar campos obligatorios"
print("Mensajes disponibles en Bronze:", muestra.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Leer Bronze en streaming
# MAGIC La primera ejecución procesa los registros existentes. Las siguientes continúan desde el checkpoint. Se aplica la misma función de limpieza usada en la revisión.
# MAGIC
# MAGIC Este flujo espera nuevas filas insertadas mediante append en Bronze. Las actualizaciones y borrados requieren otro diseño.

# COMMAND ----------

bronze_stream = spark.readStream.table(BRONZE)
mensajes_stream = transformar_mensajes(bronze_stream)

print("¿Es streaming?", mensajes_stream.isStreaming)
mensajes_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Escribir en Silver con availableNow
# MAGIC `availableNow` procesa los datos pendientes al iniciar la ejecución y termina. `toTable` crea la tabla Delta si no existe. Se escribe una fila por mensaje, sin agregaciones.
# MAGIC
# MAGIC Conserva el mismo checkpoint y tabla destino entre ejecuciones. No lo borres ni lo compartas con procesos: contiene el progreso de esta consulta.

# COMMAND ----------

query = (
    mensajes_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .queryName("bronze_a_silver_mensajes")
    .toTable(SILVER)
)

query.awaitTermination()
print("Carga finalizada:", SILVER)
print("Último progreso:", query.lastProgress)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparado para la siguiente capa
# MAGIC La vista materializada podrá usar `timestamp_mensaje`, `tool`, `stud_id`, `in_process`, `message_id` y `message_text_en` para calcular los gráficos. Este notebook no calcula meses, recuentos por grupo ni rankings.
# MAGIC
# MAGIC La comprobación previa de campos obligatorios revisa el estado actual de Bronze; no sustituye una política de calidad para futuros registros. Se conservan los tres campos de relleno y ambos idiomas, sin corregir automáticamente errores de codificación del origen.
# MAGIC
# MAGIC Documentación: [Delta streaming](https://docs.databricks.com/aws/en/structured-streaming/delta-lake) y [availableNow](https://docs.databricks.com/aws/en/structured-streaming/tutorial).
# MAGIC
# MAGIC El notebook debe importarse y ejecutarse en tu entorno Databricks; las tablas y permisos son los de tu workspace.