from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, window, when, lit, from_unixtime, unix_timestamp, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import date_format

# REPLACE THESE WITH YOUR ACTUAL THRESHOLD VALUES FROM threshold.txt
NET_THRESHOLD = 4672.74  # !! REPLACE WITH YOUR VALUE !!
DISK_THRESHOLD = 4519.4  # !! REPLACE WITH YOUR VALUE !!

# Initialize Spark
spark = SparkSession.builder \
    .appName("NET_DISK_Analysis") \
    .getOrCreate()

# Define schemas
net_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("net_in", DoubleType(), True),
    StructField("net_out", DoubleType(), True)
])

disk_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("disk_io", DoubleType(), True)
])

# Read CSV files
net_df = spark.read.csv("net_data.csv", header=True, schema=net_schema)
disk_df = spark.read.csv("disk_data.csv", header=True, schema=disk_schema)

# Convert timestamp string to timestamp type
net_df = net_df.withColumn("ts", col("ts").cast(TimestampType()))
disk_df = disk_df.withColumn("ts", col("ts").cast(TimestampType()))

# Join Network and Disk data
joined_df = net_df.join(disk_df, on=["ts", "server_id"], how="inner")

# Apply sliding window aggregation
windowed_df = joined_df.groupBy(
    window(col("ts"), "30 seconds", "10 seconds"),
    col("server_id")
).agg(
    spark_max("net_in").alias("max_net_in"),
    spark_max("disk_io").alias("max_disk_io")
)

# Extract window start and end times
windowed_df = windowed_df.withColumn("window_start", col("window.start"))
windowed_df = windowed_df.withColumn("window_end", col("window.end"))

# Filter to keep only windows where the start time's HH:mm:ss is >= 20:54:00
# This removes irregular initial windows
windowed_df = windowed_df.filter(
    date_format(col("window_start"), "HH:mm:ss") >= "20:54:00"
)

# Round to 2 decimal places
windowed_df = windowed_df.withColumn("max_net_in", col("max_net_in").cast("decimal(10,2)"))
windowed_df = windowed_df.withColumn("max_disk_io", col("max_disk_io").cast("decimal(10,2)"))

# Apply alerting logic
alert_df = windowed_df.withColumn(
    "alert",
    when((col("max_net_in") > NET_THRESHOLD) & (col("max_disk_io") > DISK_THRESHOLD),
         "Network flood + Disk thrash suspected")
    .when((col("max_net_in") > NET_THRESHOLD) & (col("max_disk_io") <= DISK_THRESHOLD),
          "Possible DDoS")
    .when((col("max_disk_io") > DISK_THRESHOLD) & (col("max_net_in") <= NET_THRESHOLD),
          "Disk thrash suspected")
    .otherwise("")
)

# Format output
output_df = alert_df.select(
    col("server_id"),
    date_format(col("window_start"), "HH:mm:ss").alias("window_start"),
    date_format(col("window_end"), "HH:mm:ss").alias("window_end"),
    col("max_net_in"),
    col("max_disk_io"),
    col("alert")
).orderBy("server_id", "window_start")

# Show results
print("=== Network + Disk Alerts ===")
output_df.show(50, truncate=False)

# Save to CSV
output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("temp_output2")

# Rename output file
import os
import shutil

# Find the generated CSV file
temp_files = [f for f in os.listdir("temp_output2") if f.endswith(".csv")]
if temp_files:
    shutil.copy(f"temp_output2/{temp_files[0]}", "team_157_NET_DISK.csv")
    print(f"\nOutput saved to: team_157_NET_DISK.csv")

shutil.rmtree("temp_output2")
spark.stop()
