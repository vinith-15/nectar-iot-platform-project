import great_expectations as gx
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataQuality").getOrCreate()
bronze_path = "/opt/bitnami/spark/data/delta/bronze"
bronze_df = spark.read.format("delta").load(bronze_path).limit(1000)
pdf = bronze_df.toPandas()

context = gx.get_context()
datasource = context.sources.add_pandas(name="pandas_datasource")
data_asset = datasource.add_dataframe_asset(name="telemetry", dataframe=pdf)
batch_request = data_asset.build_batch_request()

suite = context.add_expectation_suite("iot_telemetry_suite")
expectations = [
    gx.expectations.ExpectColumnValuesToNotBeNull(column="timestamp"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="site_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="asset_id"),
    gx.expectations.ExpectColumnValuesToBeBetween(column="temperature", min_value=-10, max_value=50),
    gx.expectations.ExpectColumnValuesToBeBetween(column="humidity", min_value=0, max_value=100),
    gx.expectations.ExpectColumnValuesToBeBetween(column="power_consumption", min_value=0, max_value=1000),
    gx.expectations.ExpectColumnValuesToBeInSet(column="operating_mode", value_set=["Normal", "Idle", "Maintenance", "Fault"]),
]
for exp in expectations:
    suite.add_expectation(exp)

validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)
checkpoint = context.add_or_update_checkpoint(name="telemetry_checkpoint", validator=validator)
result = checkpoint.run()
print(result)
print("Data quality report generated.")
