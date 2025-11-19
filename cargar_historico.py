from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip

# 1. Creamos el builder y AÑADIMOS MANUALMENTE las configs que pide el error
builder = SparkSession.builder.appName("CargaHistoricoCSV") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# 2. Usamos el helper para que descargue los Jars necesarios
spark = configure_spark_with_delta_pip(builder).getOrCreate()

ruta_csv = "DanaPP/datasets/transacciones_con_diferencias_reales.csv"

df_historico = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(ruta_csv)

print(f"Se leyeron {df_historico.count()} filas del CSV.")
df_historico.printSchema()

ruta_delta = "/home/zidnz/DanaPP/proyecto_fraude/deltaLake"

df_historico.write.format("delta") \
    .mode("overwrite") \
    .save(ruta_delta)

print(f"¡Datos guardados exitosamente como tabla Delta en: {ruta_delta}")

spark.stop()