# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark: primeros pasos en Databricks
# MAGIC **Formación práctica · 60 minutos · Nivel inicial**
# MAGIC
# MAGIC Aprenderás a explorar un DataFrame, filtrar y transformar filas, calcular métricas y combinar datos.
# MAGIC
# MAGIC **Requisitos:** conocimientos básicos de Python. No se necesita experiencia con Spark ni archivos externos.
# MAGIC
# MAGIC ## Guion de la sesión
# MAGIC | Minutos | Actividad |
# MAGIC |---|---|
# MAGIC | 0–5 | Spark, DataFrames y ejecución diferida |
# MAGIC | 5–10 | Crear y explorar datos |
# MAGIC | 10–20 | Seleccionar, filtrar y transformar |
# MAGIC | 20–30 | Agregar y combinar |
# MAGIC | 30–55 | Cuatro ejercicios |
# MAGIC | 55–60 | Repaso y dudas |
# MAGIC
# MAGIC ## Antes de empezar
# MAGIC Importa este archivo `.ipynb` en tu workspace de Databricks, selecciona un recurso de cómputo compatible con Python y Spark y espera a que esté disponible.
# MAGIC
# MAGIC Ejecuta las celdas de arriba abajo con **Shift + Enter**. Detente al llegar a los ejercicios: las soluciones están al final. Las celdas de ejercicios contienen comentarios para que «Ejecutar todo» no produzca errores por código incompleto.
# MAGIC
# MAGIC Los datos son ficticios y se crean en memoria. No se escriben tablas ni archivos.

# COMMAND ----------

# MAGIC %md
# MAGIC # Parte 1 · Teoría con ejemplos (30 min)
# MAGIC ## 1. Spark y DataFrames · 5 min
# MAGIC **Apache Spark** es un motor de procesamiento que puede distribuir el trabajo entre varias máquinas. **PySpark** permite utilizarlo desde Python.
# MAGIC
# MAGIC Un **DataFrame** representa datos organizados en filas y columnas, con un esquema que define nombres y tipos. Aunque los datos del taller son pequeños, las operaciones que aprenderemos se utilizan también con grandes volúmenes.
# MAGIC
# MAGIC En Databricks, la variable **`spark`** da acceso a la sesión de Spark y ya está disponible.
# MAGIC
# MAGIC - Una **transformación**, como `select()` o `filter()`, devuelve un nuevo DataFrame y construye un plan de cálculo.
# MAGIC - Una **acción**, como `show()` o `count()`, solicita un resultado y desencadena la ejecución necesaria.
# MAGIC - Spark utiliza **evaluación diferida**: las transformaciones no calculan inmediatamente todos sus resultados.
# MAGIC
# MAGIC Los DataFrames son inmutables: transformar uno no modifica el original. Reasignar una variable solo cambia a qué DataFrame apunta.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# En Databricks se reutiliza la sesión existente.
print("Versión de Spark:", spark.version)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear y explorar · 5 min
# MAGIC Trabajaremos con dos DataFrames:
# MAGIC
# MAGIC - **`ventas`**: una fila por línea de venta. `id_venta` es único en este ejemplo.
# MAGIC - **`clientes`**: una fila por cliente, con identificador único.
# MAGIC
# MAGIC Cada línea tiene producto, categoría, unidades y precio unitario en euros. El cliente 99 no aparece en el catálogo: nos servirá para observar un `left join`.
# MAGIC
# MAGIC Usamos un **esquema explícito** para controlar los tipos. `nullable=False` indica que el campo no admite nulos en estos datos de entrada. En este taller usamos `DoubleType` por sencillez; para importes monetarios de producción conviene definir precisión y escala con `DecimalType`.

# COMMAND ----------

esquema_ventas = StructType([
    StructField("id_venta", IntegerType(), False),
    StructField("id_cliente", IntegerType(), False),
    StructField("producto", StringType(), False),
    StructField("categoria", StringType(), False),
    StructField("unidades", IntegerType(), False),
    StructField("precio_unitario", DoubleType(), False),
])

datos_ventas = [
    (1, 1, "Portátil", "Tecnología", 1, 800.0),
    (2, 2, "Ratón", "Tecnología", 2, 25.0),
    (3, 1, "Cuaderno", "Papelería", 5, 4.0),
    (4, 3, "Monitor", "Tecnología", 1, 200.0),
    (5, 2, "Bolígrafo", "Papelería", 10, 1.5),
    (6, 4, "Teclado", "Tecnología", 1, 60.0),
    (7, 3, "Cuaderno", "Papelería", 3, 4.0),
    (8, 1, "Ratón", "Tecnología", 1, 25.0),
    (9, 4, "Mochila", "Accesorios", 2, 30.0),
    (10, 99, "Monitor", "Tecnología", 1, 200.0),
    (11, 2, "Mochila", "Accesorios", 1, 30.0),
    (12, 3, "Teclado", "Tecnología", 2, 60.0),
]
ventas = spark.createDataFrame(datos_ventas, esquema_ventas)

clientes = spark.createDataFrame(
    [(1, "Ana", "Madrid"), (2, "Luis", "Sevilla"),
     (3, "Marta", "Valencia"), (4, "Pablo", "Bilbao"),
     (5, "Elena", "Zaragoza")],
    "id_cliente INT, nombre STRING, ciudad STRING"
)


# COMMAND ----------

ventas.printSchema()
ventas.show(5, truncate=False)
print("Número de líneas de venta:", ventas.count())
print("Columnas:", ventas.columns)


# COMMAND ----------

# display() es una utilidad de Databricks para explorar resultados en una tabla.
display(ventas.orderBy("id_venta"))


# COMMAND ----------

# MAGIC %md
# MAGIC **Qué observar:** hay 12 filas y 6 columnas. `show(5)` solo muestra cinco filas; no limita el DataFrame original. Sin `orderBy()`, el orden de las filas no está garantizado.
# MAGIC
# MAGIC `display()` y `show()` solicitan datos para mostrarlos. Evita traer todo un conjunto grande a Python con `collect()` o `toPandas()`: el resultado debe caber en la memoria del proceso que lo recibe.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Seleccionar, filtrar y transformar · 10 min
# MAGIC ### Seleccionar columnas
# MAGIC `select()` elige columnas o expresiones. `F.col()` representa una columna de Spark; no es una lista de Python. `alias()` cambia el nombre de una columna en el resultado.
# MAGIC
# MAGIC Usamos el alias **`F`** para identificar las funciones de PySpark y evitar confundirlas con funciones de Python.

# COMMAND ----------

seleccion = ventas.select(
    "id_venta", "producto", F.col("precio_unitario").alias("precio_eur")
)
seleccion.orderBy("id_venta").show(4, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtrar y ordenar
# MAGIC `filter()` conserva las filas cuya condición es verdadera. Combina condiciones con **`&`** (y), **`|`** (o) y **`~`** (no), poniendo cada condición entre paréntesis. No uses `and` u `or` de Python con columnas de Spark.
# MAGIC
# MAGIC `orderBy()` ordena el resultado; `desc()` indica orden descendente.

# COMMAND ----------

ventas_filtradas = ventas.filter(
    (F.col("categoria") == "Tecnología") & (F.col("unidades") >= 2)
)
ventas_filtradas.orderBy(F.col("precio_unitario").desc(), "id_venta").show()
# Resultado: ventas 12 y 2, en ese orden.


# COMMAND ----------

# MAGIC %md
# MAGIC ### Crear columnas y aplicar condiciones
# MAGIC `withColumn()` añade una columna o reemplaza otra con el mismo nombre. `when().otherwise()` expresa una condición evaluada para cada fila.
# MAGIC
# MAGIC Calcularemos el importe de cada línea y la clasificaremos como «Alta» cuando alcance 100 €. `F.lit()` crea un valor constante dentro de una expresión de Spark.

# COMMAND ----------

ventas_con_importe = ventas.withColumn(
    "importe", F.round(F.col("unidades") * F.col("precio_unitario"), 2)
)
ventas_clasificadas = ventas_con_importe.withColumn(
    "tipo_venta",
    F.when(F.col("importe") >= 100, F.lit("Alta")).otherwise(F.lit("Estándar"))
)
ventas_clasificadas.select("id_venta", "producto", "importe", "tipo_venta").orderBy("id_venta").show()
# ventas sigue teniendo sus 6 columnas originales.
print("Columnas originales:", ventas.columns)


# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta rápida:** ¿`ventas.filter(...)` calcula inmediatamente todas las filas filtradas? No: construye el plan. Al llamar a `show()` se solicita la ejecución necesaria para mostrar resultados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resumir y combinar · 10 min
# MAGIC ### Agrupar y agregar
# MAGIC `groupBy()` define los grupos y `agg()` calcula métricas en cada grupo.
# MAGIC
# MAGIC - `F.sum("importe")`: suma de importes.
# MAGIC - `F.count("*")`: número de filas, aquí líneas de venta.
# MAGIC - `F.avg("importe")`: importe medio por línea.
# MAGIC
# MAGIC Después de agrupar, el resultado contiene una fila por grupo. El importe medio por línea no equivale al precio unitario medio.

# COMMAND ----------

resumen_categoria = (
    ventas_con_importe
    .groupBy("categoria")
    .agg(
        F.count("*").alias("lineas"),
        F.round(F.sum("importe"), 2).alias("importe_total"),
        F.round(F.avg("importe"), 2).alias("importe_medio_linea")
    )
    .orderBy(F.col("importe_total").desc())
)
resumen_categoria.show()
# Tecnología: 1455.0 €; Accesorios: 90.0 €; Papelería: 47.0 €.


# COMMAND ----------

# MAGIC %md
# MAGIC ### Combinar con un left join
# MAGIC Un **join** combina filas mediante una clave común.
# MAGIC
# MAGIC | Tipo | Filas que conserva |
# MAGIC |---|---|
# MAGIC | `inner` | Solo las que tienen coincidencia en ambos lados |
# MAGIC | `left` | Todas las del lado izquierdo y sus coincidencias del derecho |
# MAGIC
# MAGIC Añadimos los datos del cliente a cada línea de venta. Al pasar `on="id_cliente"`, la clave común aparece una sola vez en el resultado.
# MAGIC
# MAGIC Si la clave del lado derecho tiene duplicados, un join puede multiplicar filas. Aquí `clientes.id_cliente` es único.

# COMMAND ----------

ventas_detalle = ventas_con_importe.join(clientes, on="id_cliente", how="left")
ventas_detalle.select(
    "id_venta", "id_cliente", "nombre", "ciudad", "importe"
).orderBy("id_venta").show()

# La venta del cliente 99 se conserva, con nombre y ciudad NULL.
ventas_detalle.filter(F.col("nombre").isNull()).show()


# COMMAND ----------

# MAGIC %md
# MAGIC **Qué observar:** el resultado conserva las 12 líneas. El cliente 99 no tiene nombre ni ciudad. Elena (cliente 5) no aparece porque no tiene ventas y `ventas_con_importe` es el lado izquierdo.
# MAGIC
# MAGIC Para buscar nulos utiliza `isNull()` o `isNotNull()`, en lugar de comparar con `None`.
# MAGIC
# MAGIC **Pausa antes de practicar:** identifica una transformación, una acción y la clave utilizada en el join.

# COMMAND ----------

# MAGIC %md
# MAGIC # Parte 2 · Ejercicios (25 min)
# MAGIC Trabaja a partir de **`ventas`** y **`clientes`**, creados al principio. No necesitas reutilizar los resultados de los ejemplos.
# MAGIC
# MAGIC Cada ejercicio indica el nombre de su resultado para facilitar la puesta en común. Puedes usar las pistas si te bloqueas. Las soluciones están en la siguiente sección.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 1 · Seleccionar, filtrar y ordenar (5 min)
# MAGIC Obtén las ventas de la categoría **Tecnología** con precio unitario **mayor o igual a 60 €**.
# MAGIC
# MAGIC 1. Conserva `id_venta`, `producto`, `unidades` y `precio_unitario`.
# MAGIC 2. Ordena por `precio_unitario` descendente y, en caso de empate, por `id_venta` ascendente.
# MAGIC 3. Guarda el resultado en `ej1` y muéstralo.
# MAGIC
# MAGIC **Comprobación:** deben aparecer las ventas **1, 4, 10, 6 y 12**, en ese orden.
# MAGIC
# MAGIC **Pista:** combina dos condiciones entre paréntesis con `&`.

# COMMAND ----------

# Escribe aquí tu solución.
# ej1 = ...
# ej1.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 2 · Calcular y clasificar (6 min)
# MAGIC Partiendo de todas las filas de `ventas`:
# MAGIC
# MAGIC 1. Crea `importe` como unidades × precio unitario, redondeado a dos decimales.
# MAGIC 2. Crea `tipo_venta`: «Alta» si `importe >= 100`; «Estándar» en el resto.
# MAGIC 3. Guarda el resultado en `ej2` y muestra las columnas necesarias para revisarlo.
# MAGIC
# MAGIC **Comprobación:** la venta 2 tiene importe **50.0** y tipo «Estándar»; la venta 12 tiene importe **120.0** y tipo «Alta». Hay **4** líneas clasificadas como «Alta».
# MAGIC
# MAGIC **Pista:** encadena dos llamadas a `withColumn()` y usa `F.when()`.

# COMMAND ----------

# Escribe aquí tu solución.
# ej2 = ...
# ej2.orderBy("id_venta").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 3 · Calcular ventas por cliente (7 min)
# MAGIC A partir de `ej2`:
# MAGIC
# MAGIC 1. Agrupa por `id_cliente`.
# MAGIC 2. Calcula `numero_lineas` y `total_ventas` (suma de `importe`, redondeada a dos decimales).
# MAGIC 3. Ordena de mayor a menor `total_ventas` y guarda el resultado en `ej3`.
# MAGIC
# MAGIC **Comprobación:** debes obtener 5 filas. El cliente 1 tiene **3 líneas** y **845.0 €**. La suma de los totales de todos los clientes es **1592.0 €**.
# MAGIC
# MAGIC **Pista:** usa `groupBy().agg()`. Aquí contamos líneas, no unidades.

# COMMAND ----------

# Escribe aquí tu solución.
# ej3 = ...
# ej3.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 4 · Añadir nombres sin perder ventas (7 min)
# MAGIC Combina `ej3` con `clientes` mediante un **left join**, manteniendo `ej3` a la izquierda.
# MAGIC
# MAGIC 1. Conserva `id_cliente`, `nombre`, `numero_lineas` y `total_ventas`.
# MAGIC 2. Ordena por `total_ventas` descendente y guarda el resultado en `ej4`.
# MAGIC 3. Muestra también qué filas no tienen nombre de cliente.
# MAGIC
# MAGIC **Comprobación:** quedan 5 filas. El cliente 99 sigue presente, con nombre nulo y **200.0 €**. Elena no aparece.
# MAGIC
# MAGIC **Pista:** `join(..., on="id_cliente", how="left")` e `isNull()`.

# COMMAND ----------

# Escribe aquí tu solución.
# ej4 = ...
# ej4.show()
# ej4.filter(...).show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Soluciones · Consultar después de los ejercicios
# MAGIC Estas celdas son ejecutables y reconstruyen los resultados en orden. Ejecuta primero la creación de datos de la parte 1. Se usan variables `sol1` a `sol4` para conservar tus respuestas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solución 1
# MAGIC Primero filtramos para mantener disponible la columna `categoria`; después seleccionamos las columnas solicitadas.

# COMMAND ----------

sol1 = (
    ventas
    .filter((F.col("categoria") == "Tecnología") & (F.col("precio_unitario") >= 60))
    .select("id_venta", "producto", "unidades", "precio_unitario")
    .orderBy(F.col("precio_unitario").desc(), F.col("id_venta").asc())
)
sol1.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Solución 2
# MAGIC La segunda transformación puede utilizar `importe` porque la primera ya lo incorpora al nuevo DataFrame.

# COMMAND ----------

sol2 = (
    ventas
    .withColumn("importe", F.round(F.col("unidades") * F.col("precio_unitario"), 2))
    .withColumn("tipo_venta", F.when(F.col("importe") >= 100, "Alta").otherwise("Estándar"))
)
sol2.select("id_venta", "importe", "tipo_venta").orderBy("id_venta").show()
print("Líneas de tipo Alta:", sol2.filter(F.col("tipo_venta") == "Alta").count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Solución 3
# MAGIC `F.count("*")` cuenta filas. Para contar unidades vendidas utilizaríamos `F.sum("unidades")`.

# COMMAND ----------

sol3 = (
    sol2.groupBy("id_cliente")
    .agg(
        F.count("*").alias("numero_lineas"),
        F.round(F.sum("importe"), 2).alias("total_ventas")
    )
    .orderBy(F.col("total_ventas").desc())
)
sol3.show()
# Orden esperado: cliente 1 (845), 3 (332), 99 (200), 4 (120), 2 (95).
sol3.agg(F.round(F.sum("total_ventas"), 2).alias("total_general")).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Solución 4
# MAGIC El `left join` conserva cada cliente con ventas aunque falten sus datos en el catálogo. Volvemos a ordenar después del join: el orden anterior no está garantizado tras combinar.

# COMMAND ----------

sol4 = (
    sol3.join(clientes, on="id_cliente", how="left")
    .select("id_cliente", "nombre", "numero_lineas", "total_ventas")
    .orderBy(F.col("total_ventas").desc())
)
sol4.show()
sol4.filter(F.col("nombre").isNull()).show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Cierre · 5 min
# MAGIC ## Lo esencial
# MAGIC | Necesidad | Operación |
# MAGIC |---|---|
# MAGIC | Ver estructura y datos | `printSchema()`, `show()`, `display()` |
# MAGIC | Elegir columnas | `select()` |
# MAGIC | Conservar filas por una condición | `filter()` |
# MAGIC | Añadir o reemplazar una columna | `withColumn()` |
# MAGIC | Aplicar una condición por fila | `F.when().otherwise()` |
# MAGIC | Ordenar filas | `orderBy()` |
# MAGIC | Resumir grupos | `groupBy().agg()` |
# MAGIC | Combinar datos | `join()` |
# MAGIC
# MAGIC ## Preguntas de repaso
# MAGIC 1. ¿Qué diferencia hay entre una transformación y una acción?
# MAGIC 2. ¿Por qué usamos `&` en lugar de `and` al combinar condiciones sobre columnas?
# MAGIC 3. ¿Qué sucedería con el cliente 99 si el último join fuera `inner`?
# MAGIC
# MAGIC **Respuestas:** una transformación construye un nuevo plan y una acción solicita resultados; `&` combina expresiones de columna de Spark; con `inner` desaparecería el cliente 99 y quedarían 4 filas.
# MAGIC