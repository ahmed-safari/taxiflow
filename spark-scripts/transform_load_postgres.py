"""
NYC Green Taxi Data Transformation Script
==========================================
This script reads raw parquet data from MinIO, applies 12+ advanced transformations,
performs data quality filtering, and loads the cleaned data into PostgreSQL.

Transformations Applied:
1. trip_hour - Hour of pickup (0-23)
2. trip_weekday - Day of week (1=Sunday, 7=Saturday)
3. trip_month - Month of pickup
4. trip_day - Day of month
5. trip_duration_min - Duration in minutes
6. time_of_day - Category (Morning/Afternoon/Evening/Night)
7. is_weekend - Boolean flag
8. day_type - Weekend/Weekday
9. avg_speed_mph - Average speed in mph
10. fare_per_mile - Cost per mile
11. fare_per_minute - Cost per minute
12. distance_category - Short/Medium/Long/Very Long
13. fare_category - Budget/Standard/Premium/Luxury
14. tip_percentage - Tip as percentage of fare
15. payment_type_name - Human-readable payment type
16. vendor_name - Human-readable vendor name

Author: Ahmed Yousef
Course: DSAI5102 Data Architecture & Engineering
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, hour, dayofweek, when, lit, round as spark_round,
    month, dayofmonth, year, date_format, coalesce, abs as spark_abs
)
from pyspark.sql.types import DoubleType, IntegerType
import os

# ============================================================
# CONFIGURATION
# ============================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sampledb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "demo")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "demo")

RAW_PATH = "s3a://datalake/nyc_green/raw/green_tripdata_2024-01.parquet"
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("NYC_GreenTaxi_Transform_Load") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# ============================================================
# READ RAW DATA FROM MINIO
# ============================================================
print("=" * 60)
print("NYC Green Taxi ETL - Transform & Load")
print("=" * 60)
print("\nReading raw data from MinIO...")
df = spark.read.parquet(RAW_PATH)
raw_count = df.count()
print(f"✓ Raw records loaded: {raw_count:,}")

# ============================================================
# ADVANCED TRANSFORMATIONS
# ============================================================
print("\nApplying advanced transformations...")

# Convert timestamps
df2 = df.withColumn("pickup_datetime", to_timestamp(col("lpep_pickup_datetime")))
df2 = df2.withColumn("dropoff_datetime", to_timestamp(col("lpep_dropoff_datetime")))

# TIME-BASED FEATURES
df2 = df2.withColumn("trip_hour", hour(col("lpep_pickup_datetime")))
df2 = df2.withColumn("trip_weekday", dayofweek(col("lpep_pickup_datetime")))
df2 = df2.withColumn("trip_month", month(col("lpep_pickup_datetime")))
df2 = df2.withColumn("trip_day", dayofmonth(col("lpep_pickup_datetime")))

# DURATION CALCULATION
df2 = df2.withColumn("trip_duration_min",
    (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60)

# TIME OF DAY CATEGORY
df2 = df2.withColumn("time_of_day",
    when((col("trip_hour") >= 6) & (col("trip_hour") < 12), "Morning")
    .when((col("trip_hour") >= 12) & (col("trip_hour") < 17), "Afternoon")
    .when((col("trip_hour") >= 17) & (col("trip_hour") < 21), "Evening")
    .otherwise("Night"))

# DAY TYPE (WEEKEND/WEEKDAY)
df2 = df2.withColumn("is_weekend",
    when(col("trip_weekday").isin(1, 7), True).otherwise(False))
df2 = df2.withColumn("day_type",
    when(col("trip_weekday").isin(1, 7), "Weekend").otherwise("Weekday"))

# SPEED CALCULATION (mph)
df2 = df2.withColumn("avg_speed_mph",
    when((col("trip_duration_min") > 0) & (col("trip_distance") > 0),
         spark_round(col("trip_distance") / (col("trip_duration_min") / 60), 2))
    .otherwise(None))

# FARE PER MILE
df2 = df2.withColumn("fare_per_mile",
    when(col("trip_distance") > 0,
         spark_round(col("total_amount") / col("trip_distance"), 2))
    .otherwise(None))

# FARE PER MINUTE
df2 = df2.withColumn("fare_per_minute",
    when(col("trip_duration_min") > 0,
         spark_round(col("total_amount") / col("trip_duration_min"), 2))
    .otherwise(None))

# TRIP DISTANCE CATEGORY
df2 = df2.withColumn("distance_category",
    when(col("trip_distance") <= 1, "Short (<1 mi)")
    .when((col("trip_distance") > 1) & (col("trip_distance") <= 5), "Medium (1-5 mi)")
    .when((col("trip_distance") > 5) & (col("trip_distance") <= 10), "Long (5-10 mi)")
    .otherwise("Very Long (>10 mi)"))

# FARE CATEGORY
df2 = df2.withColumn("fare_category",
    when(col("total_amount") <= 10, "Budget (<$10)")
    .when((col("total_amount") > 10) & (col("total_amount") <= 25), "Standard ($10-25)")
    .when((col("total_amount") > 25) & (col("total_amount") <= 50), "Premium ($25-50)")
    .otherwise("Luxury (>$50)"))

# TIP PERCENTAGE
df2 = df2.withColumn("tip_percentage",
    when((col("total_amount") > 0) & (col("tip_amount") > 0),
         spark_round((col("tip_amount") / (col("total_amount") - col("tip_amount"))) * 100, 2))
    .otherwise(0))

# PAYMENT TYPE NAME
df2 = df2.withColumn("payment_type_name",
    when(col("payment_type") == 1, "Credit Card")
    .when(col("payment_type") == 2, "Cash")
    .when(col("payment_type") == 3, "No Charge")
    .when(col("payment_type") == 4, "Dispute")
    .when(col("payment_type") == 5, "Unknown")
    .otherwise("Other"))

# VENDOR NAME
df2 = df2.withColumn("vendor_name",
    when(col("VendorID") == 1, "Creative Mobile Technologies")
    .when(col("VendorID") == 2, "VeriFone Inc.")
    .otherwise("Unknown"))

print("✓ Applied 16 transformations")

# ============================================================
# DATA QUALITY FILTERING
# ============================================================
print("\nApplying data quality filters...")

df_clean = df2.filter(
    # Remove negative or zero fares
    (col("total_amount") > 0) &
    # Remove unrealistic distances (negative or >100 miles)
    (col("trip_distance") >= 0) & (col("trip_distance") < 100) &
    # Remove unrealistic durations (negative or >4 hours)
    (col("trip_duration_min") > 0) & (col("trip_duration_min") < 240) &
    # Remove unrealistic speeds (>100 mph likely GPS error)
    ((col("avg_speed_mph").isNull()) | (col("avg_speed_mph") < 100))
)

clean_count = df_clean.count()
removed_count = raw_count - clean_count
print(f"✓ Removed {removed_count:,} invalid records ({removed_count/raw_count*100:.2f}%)")
print(f"✓ Clean records: {clean_count:,}")

# ============================================================
# SELECT ANALYTICS COLUMNS
# ============================================================
fact_trips = df_clean.select(
    "VendorID",
    "vendor_name",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance",
    "total_amount",
    "tip_amount",
    "payment_type",
    "payment_type_name",
    "trip_hour",
    "trip_weekday",
    "trip_month",
    "trip_day",
    "trip_duration_min",
    "time_of_day",
    "is_weekend",
    "day_type",
    "avg_speed_mph",
    "fare_per_mile",
    "fare_per_minute",
    "distance_category",
    "fare_category",
    "tip_percentage",
)

# ============================================================
# LOAD TO POSTGRESQL
# ============================================================
print(f"\nLoading data into PostgreSQL ({POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB})...")

# Use truncate option to clear table without dropping it (preserves dependent views)
fact_trips.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", "fact_trips") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .option("truncate", "true") \
    .mode("overwrite") \
    .save()

print("✓ Data successfully saved to PostgreSQL table: fact_trips")
print("\n" + "=" * 60)
print("ETL COMPLETE!")
print("=" * 60)

spark.stop()
