from pyspark.sql import SparkSession
from delta import *

spark = SparkSession.builder \
    .appName("LoadPostgres") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:3.0.0") \
    .getOrCreate()

gold_path = "/opt/bitnami/spark/data/delta/gold/hourly_energy"
df = spark.read.format("delta").load(gold_path)

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/nectar") \
    .option("dbtable", "fact_energy") \
    .option("user", "admin") \
    .option("password", "admin123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("Data loaded to PostgreSQL.")
