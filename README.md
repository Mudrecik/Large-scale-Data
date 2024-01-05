# Large-scale-Data
Large-scale Data Processing with Apache Spark:
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Create a Spark session
spark = SparkSession.builder.appName("data_processing").getOrCreate()

# Load data from a source
data = spark.read.csv("data.csv", header=True, inferSchema=True)

# Data processing: filtering, transformation, aggregation
filtered_data = data.filter(col("column") > 100)
transformed_data = filtered_data.withColumn("new_column", udf_function(col("column")))
aggregated_data = transformed_data.groupBy("category").agg({"value": "avg"})

# Save the results
aggregated_data.write.csv("output_data")

# Stop the Spark session
spark.stop()
