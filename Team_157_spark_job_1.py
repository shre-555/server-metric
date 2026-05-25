from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, when, lit, date_format, min as spark_min, max as spark_max, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# !! ACTION REQUIRED: REPLACE THESE WITH YOUR ACTUAL THRESHOLD VALUES FROM threshold.txt !!
CPU_THRESHOLD = 90.73  # Replace with your value
MEM_THRESHOLD = 77.73  # Replace with your value

# Initialize Spark
spark = SparkSession.builder \
    .appName("CPU_MEM_Analysis") \
    .getOrCreate()

# Define schemas
cpu_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("cpu_pct", FloatType(), True)
])

mem_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("mem_pct", FloatType(), True)
])

# Read CSV files
cpu_df = spark.read.csv("cpu_data.csv", header=True, schema=cpu_schema)
mem_df = spark.read.csv("mem_data.csv", header=True, schema=mem_schema)

# Convert timestamp string to timestamp type
cpu_df = cpu_df.withColumn("ts", col("ts").cast(TimestampType()))
mem_df = mem_df.withColumn("ts", col("ts").cast(TimestampType()))

# Join CPU and Memory data
joined_df = cpu_df.join(mem_df, on=["ts", "server_id"], how="inner")

# Find the minimum and maximum timestamps to determine proper window alignment
min_ts = joined_df.agg(spark_min("ts")).collect()[0][0]
max_ts = joined_df.agg(spark_max("ts")).collect()[0][0]

# Apply sliding window aggregation
# Window: 30 seconds, Slide: 10 seconds
windowed_df = joined_df.groupBy(
    window(col("ts"), "30 seconds", "10 seconds"),
    col("server_id")
).agg(
    avg("cpu_pct").alias("avg_cpu"),
    avg("mem_pct").alias("avg_mem")
)

# Round to 2 decimal places using round() function
windowed_df = windowed_df.withColumn("avg_cpu", spark_round(col("avg_cpu"), 2))
windowed_df = windowed_df.withColumn("avg_mem", spark_round(col("avg_mem"), 2))

# Filter out windows that start before the minimum timestamp
# This removes the misaligned first windows that cause the extra rows
windowed_df = windowed_df.filter(col("window.start") >= lit(min_ts))

# Apply alerting logic
alert_df = windowed_df.withColumn(
    "alert",
    when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") > MEM_THRESHOLD),
         "High CPU + Memory stress")
    .when((col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") <= MEM_THRESHOLD),
          "CPU spike suspected")
    .when((col("avg_mem") > MEM_THRESHOLD) & (col("avg_cpu") <= CPU_THRESHOLD),
          "Memory saturation suspected")
    .otherwise("")
)

# Format output and sort
output_df = alert_df.select(
    col("server_id"),
    # Format to HH:mm:ss string for portal submission
    date_format(col("window.start"), "HH:mm:ss").alias("window_start"), 
    date_format(col("window.end"), "HH:mm:ss").alias("window_end"),     
    col("avg_cpu"),
    col("avg_mem"),
    col("alert")
# Sort by server_id (primary) and window_start (secondary)
).orderBy("server_id", "window_start") 

# Show results
print("=== CPU + Memory Alerts ===")
output_df.show(50, truncate=False)

# Count rows for verification
row_count = output_df.count()
print(f"\nTotal rows in output: {row_count}")

# Save to CSV
output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("temp_output1")

# Rename output file
import os
import shutil

# Find the generated CSV file
temp_files = [f for f in os.listdir("temp_output1") if f.endswith(".csv")]
if temp_files:
    shutil.copy(f"temp_output1/{temp_files[0]}", "team_157_CPU_MEM.csv")
    print(f"\nOutput saved to: team_157_CPU_MEM.csv")

shutil.rmtree("temp_output1")
spark.stop()

