# consumidor_con_modelo_spark.py
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, hour, log, avg, udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
    LongType,
    FloatType,
)
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.ml.linalg import Vector

# === 1. Configurar Spark Session ===
print("Iniciando SparkSession para Detección de Fraude...")
spark = (
    SparkSession.builder.appName("DetectorFraudeStreaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# === 2. Definir el Schema del JSON de Kafka ===
# ¡Este schema DEBE coincidir con tu productor.py!
schema = StructType(
    [
        StructField("id_transaccion", IntegerType(), True),
        StructField("fecha_hora", StringType(), True), # Lo leeremos como string
        StructField("monto", DoubleType(), True),
        StructField("tipo", StringType(), True),      # Columna cruda 'tipo'
        StructField("ubicacion", StringType(), True), # Columna cruda 'ubicacion'
        StructField("dispositivo", StringType(), True),# Columna cruda 'dispositivo'
        StructField("id_cuenta", IntegerType(), True),
        StructField("tipo_cuenta", StringType(), True),
        StructField("saldo_actual", DoubleType(), True),
        StructField("estatus", StringType(), True),
        StructField("id_cliente", IntegerType(), True),
        StructField("nombre", StringType(), True),
        StructField("correo", StringType(), True),
        StructField("telefono", LongType(), True), # 'telefono' es un número largo
        StructField("fecha_registro", StringType(), True),
        StructField("nivel_riesgo", StringType(), True),
    ]
)

# === 3. Cargar el Modelo de Spark ML ===
# Ruta real a tu modelo guardado
MODEL_PATH = "/home/zidnz/DanaPP/proyecto_fraude/modelos/random_forest_fraude_cv"

print(f"✅ Cargando modelo de Spark ML desde: {MODEL_PATH}")
cv_model = CrossValidatorModel.load(MODEL_PATH)


# === 4. Lógica de Feature Engineering ===
# Esta función aplica TODAS las transformaciones del notebook en tiempo real
def aplicar_feature_engineering(df):
    
    # --- Renombrar columnas para que coincidan con el modelo ---
    df = df.withColumnRenamed("tipo", "tipo_transaccion") \
           .withColumnRenamed("dispositivo", "canal_transaccion") \
           .withColumnRenamed("ubicacion", "ciudad")  # <-- ¡ESTA ES LA CORRECCIÓN!
           
    # --- Asignar valores default a columnas faltantes ---
    # Tu modelo espera estas columnas, pero el productor no las envía.
    df = df.withColumn("categoria", when(col("tipo_transaccion") == 'compra', "Consumo").otherwise("Transferencia per..."))
    df = df.withColumn("divisa", when(col("monto") > 0, "MXN").otherwise("MXN"))
    df = df.withColumn("medio_pago", when(col("canal_transaccion") == 'cajero_automatico', "Tarjeta debito").otherwise("App movil"))

    
    # --- Re-crear TODAS las features del notebook ---
    
    # 0. Columnas de tiempo
    df_con_tiempo = df.withColumn("hora_del_dia", hour(col("fecha_hora").cast(TimestampType())))

    # 1. Banderas de Riesgo
    df_features = df_con_tiempo.withColumn("feat_horario_riesgo", 
        when(col("hora_del_dia").between(0, 4), 1).otherwise(0)
    ).withColumn("feat_tipo_riesgo", 
        # 'transferencia' (minúscula) del productor
        when(col("tipo_transaccion").isin("transferencia", "Transferencia internacional"), 1).otherwise(0) 
    ).withColumn("feat_canal_riesgo", 
        # Ajustado a los valores del productor
        when(col("canal_transaccion").isin("Sucursal bancaria", "web_chrome", "movil_ios", "movil_android"), 1).otherwise(0)
    )

    # 2. "Golden Feature" (Interacción)
    df_features = df_features.withColumn("feat_perfil_riesgo_completo", 
        when((col("feat_horario_riesgo") == 1) & 
             (col("feat_tipo_riesgo") == 1) & 
             (col("feat_canal_riesgo") == 1), 1).otherwise(0)
    )

    # 3. Features de Monto
    df_features = df_features.withColumn("feat_log_monto", log(col("monto") + 1))

    # --- Feature de Ratio (Importante) ---
    # Hard-codeamos los promedios del EDA, ya que no podemos calcularlos en el stream
    # ¡DEBES USAR TUS PROMEDIOS REALES DE TU EDA!
    df_features = df_features.withColumn("monto_promedio_tipo", 
        when(col("tipo_transaccion") == 'compra', 500.0)
        .when(col("tipo_transaccion") == 'retiro', 1500.0)
        .when(col("tipo_transaccion") == 'transferencia', 8000.0)
        .when(col("tipo_transaccion") == 'deposito', 4000.0)
        .otherwise(col("monto")) # Default
    )
    
    df_features = df_features.withColumn("feat_ratio_monto_vs_tipo", 
        when(col("monto_promedio_tipo") > 0, col("monto") / col("monto_promedio_tipo"))
        .otherwise(0)
    )
    
    return df_features

# === 5. Conectarse al Stream de Kafka ===
print("🔌 Conectando a Kafka (tópico: transacciones_bancarias)...")
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transacciones_bancarias")
    .option("startingOffsets", "latest")
    .load()
)

# === 6. Parsear el JSON de Kafka ===
transacciones_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# === 7. Aplicar Feature Engineering AL STREAM ===
df_features_stream = aplicar_feature_engineering(transacciones_df)

# === 8. Aplicar el Modelo ===
print("🤖 Aplicando modelo de detección de fraude al stream...")
predictions_df = cv_model.transform(df_features_stream)

# === 9. Formatear la Salida ===
# (UDF para extraer la probabilidad del vector)
@udf(returnType=FloatType())
def extract_fraud_probability(prob_vector: Vector) -> float:
    try: return float(prob_vector[1])
    except: return 0.0

output_df = predictions_df.select(
    col("id_transaccion"),
    col("monto"),
    col("tipo_transaccion").alias("tipo"), # Devolver al nombre original
    extract_fraud_probability(col("probability")).alias("prob_fraude"),
    when(col("prediction") == 1, "🚨 ¡¡¡FRAUDE DETECTADO!!!").otherwise("✅ Transaccion Normal").alias("Resultado")
)

# === 10. Iniciar el Stream a la Consola ===
print("🔍 Sistema de detección de fraude ACTIVO. Mostrando resultados en consola...")
query = (
    output_df.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()