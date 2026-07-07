from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, hour, current_timestamp
from delta import *

spark = SparkSession.builder \
    .appName("GoldBatch") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:3.0.0") \
    .getOrCreate()

SILVER_PATH = "/opt/bitnami/spark/data/delta/silver"
GOLD_PATH = "/opt/bitnami/spark/data/delta/gold"

silver = spark.read.format("delta").load(SILVER_PATH)
hourly_energy = silver \
    .withColumn("hour", hour(col("event_time"))) \
    .groupBy("date", "hour", "site_id", "building_id", "asset_id") \
    .agg(sum("power_kw").alias("total_energy_kwh"), avg("power_kw").alias("avg_power_kw")) \
    .withColumn("updated_at", current_timestamp())

daily_util = silver \
    .groupBy("date", "site_id", "building_id", "asset_id") \
    .agg(count("*").alias("reading_count")) \
    .withColumn("updated_at", current_timestamp())

avg_env = silver \
    .withColumn("hour", hour(col("event_time"))) \
    .groupBy("date", "hour", "site_id") \
    .agg(avg("temperature_c").alias("avg_temp_c"), avg("humidity_pct").alias("avg_humidity_pct"), avg("pressure_hpa").alias("avg_pressure_hpa")) \
    .withColumn("updated_at", current_timestamp())

hourly_energy.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/hourly_energy")
daily_util.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/daily_utilization")
avg_env.write.mode("overwrite").format("delta").save(f"{GOLD_PATH}/avg_environment")
print("Gold tables updated.")
