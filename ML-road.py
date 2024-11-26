from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofweek, hour, when
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("AccidentSeverityPrediction") \
    .getOrCreate()

# Load data from local or cloud storage (replace with your file path)
# Assuming the data is stored as a CSV file
input_path = "gs://tfl-data-bucket/processed/accident_stats_2018_cleaned/part-00000-2c8ffc3e-f435-411b-af21-841ee4a02c46-c000.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Preprocessing
# Extract date parts
df = df.withColumn("year", year(col("date"))) \
       .withColumn("month", month(col("date"))) \
       .withColumn("day_of_week", dayofweek(col("date"))) \
       .withColumn("hour", hour(col("date")))

# Handle high-cardinality categorical features
# Replace infrequent boroughs with "Other" for 'borough'
borough_counts = df.groupBy("borough").count().orderBy("count", ascending=False)
frequent_boroughs = [row["borough"] for row in borough_counts.collect() if row["count"] > 10]
df = df.withColumn(
    "borough",
    when(col("borough").isin(frequent_boroughs), col("borough")).otherwise("Other")
)

# Encode categorical features
vehicle_indexer = StringIndexer(inputCol="vehicle_type", outputCol="vehicle_type_index")
borough_indexer = StringIndexer(inputCol="borough", outputCol="borough_index")

# Assemble features
feature_columns = ["latitude", "longitude", "year", "month", "day_of_week", "hour", 
                   "vehicle_type_index", "borough_index"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Encode label
severity_indexer = StringIndexer(inputCol="accident_severity", outputCol="label")

# Model
# Adjust maxBins to handle high-cardinality features
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=100, maxBins=40)

# Pipeline
pipeline = Pipeline(stages=[vehicle_indexer, borough_indexer, assembler, severity_indexer, rf])

# Train-Test Split
train, test = df.randomSplit([0.8, 0.2])

# Train the model
model = pipeline.fit(train)

# Predictions
predictions = model.transform(test)

# Evaluation
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print(f"Test Accuracy: {accuracy:.2f}")

# Save model locally or to cloud storage
output_path = "gs://tfl-data-bucket/processed/accident_severity_model"  # Replace with your save path
model.write().overwrite().save(output_path)
