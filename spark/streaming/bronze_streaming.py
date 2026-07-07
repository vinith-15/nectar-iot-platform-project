from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when
from delta import *

spark = SparkSession.builder \
    .appName("BronzeStreaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:3.0.0") \
    .getOrCreate()

KAFKA_BROKER = "kafka:9092"
TOPIC = "iot-telemetry"
CHECKPOINT_PATH = "/opt/bitnami/spark/data/checkpoints/bronze"
BRONZE_PATH = "/opt/bitnami/spark/data/delta/bronze"

telemetry_schema = "timestamp STRING, site_id STRING, building_id STRING, asset_id STRING, sensor_id STRING, temperature DOUBLE, humidity DOUBLE, pressure DOUBLE, vibration DOUBLE, power_consumption DOUBLE, operating_mode STRING"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.select(from_json(col("value").cast("string"), telemetry_schema).alias("data")).select("data.*")
validated = parsed \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("is_valid",
                when(col("timestamp").isNull() | col("site_id").isNull() | col("asset_id").isNull(), False)
                .otherwise(True))

query = validated.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("path", BRONZE_PATH) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
