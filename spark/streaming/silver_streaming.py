from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, round
from delta import *

spark = SparkSession.builder \
    .appName("SilverStreaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:3.0.0") \
    .getOrCreate()

BRONZE_PATH = "/opt/bitnami/spark/data/delta/bronze"
SILVER_PATH = "/opt/bitnami/spark/data/delta/silver"

bronze = spark.readStream.format("delta").load(BRONZE_PATH)
silver = bronze \
    .filter(col("is_valid") == True) \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withColumn("date", date_format(col("event_time"), "yyyy-MM-dd")) \
    .withColumn("hour", date_format(col("event_time"), "HH")) \
    .withColumn("temperature_c", round(col("temperature"), 2)) \
    .withColumn("humidity_pct", round(col("humidity"), 2)) \
    .withColumn("pressure_hpa", round(col("pressure"), 2)) \
    .withColumn("vibration_mm", round(col("vibration"), 2)) \
    .withColumn("power_kw", round(col("power_consumption") / 1000, 2))

query = silver.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/bitnami/spark/data/checkpoints/silver") \
    .option("path", SILVER_PATH) \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
