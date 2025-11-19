# debug_consumidor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

print("Iniciando SparkSession de DEBUG...")
spark = SparkSession.builder.appName("DebugKafka").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Schema simple (solo para ver si lee)
schema = StructType([
    StructField("id_transaccion", StringType(), True),
    StructField("monto", DoubleType(), True),
    StructField("tipo", StringType(), True),
])

print("🔌 Conectando a Kafka (tópico: transacciones_bancarias)...")
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transacciones_bancarias")
    .option("startingOffsets", "latest")
    .load()
)

# Parsear el JSON
transacciones_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

print("🔍 Mostrando stream en consola (SIN MODELO)...")
query = (
    transacciones_df.writeStream.outputMode("append")
    .format("console")
    .start()
)

query.awaitTermination()