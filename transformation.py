from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Initialize SparkSession
spark = SparkSession.builder.appName("AccidentStats").getOrCreate()

# Define the input path
input_path = "gs://tfl-data-bucket/raw/road_traffic_incidents_2018.json"  # Replace with your GCS bucket path

# Load the JSON file with multiline option enabled
df = spark.read.option("multiline", "true").json(input_path)

# Explode the casualties and vehicles arrays
df_exploded = df \
    .withColumn("casualty", explode(col("casualties"))) \
    .withColumn("vehicle", explode(col("vehicles")))

# Select relevant fields
flat_df = df_exploded.select(
    col("id"),
    col("lat").alias("latitude"),
    col("lon").alias("longitude"),
    col("location"),
    col("date"),
    col("severity").alias("accident_severity"),
    col("borough"),
    col("casualty.age").alias("casualty_age"),
    col("casualty.class").alias("casualty_class"),
    col("casualty.severity").alias("casualty_severity"),
    col("casualty.mode").alias("casualty_mode"),
    col("casualty.ageBand").alias("casualty_ageBand"),
    col("vehicle.type").alias("vehicle_type")
)

# Save to GCS as CSV
output_path = "gs://tfl-data-bucket/processed/accident_stats_2018_cleaned"  # Replace with your GCS bucket path
flat_df.write.csv(output_path, header=True, mode="overwrite")

