from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, hour, dayofweek, when, lit, round as spark_round,
    month, dayofmonth, year, date_format, coalesce
)
import os

# ---------- LOAD CREDENTIALS FROM ENVIRONMENT ----------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sampledb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "demo")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "demo")

# ---------- CONFIG ----------
RAW_PATH = "s3a://datalake/nyc_green/raw/green_tripdata_2024-01.parquet"
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ---------- SPARK SESSION ----------
spark = SparkSession.builder \
    .appName("NYC_GreenTaxi_Transform_Load") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Reading raw data from MinIO...")
df = spark.read.parquet(RAW_PATH)
print(f"Raw records loaded: {df.count()}")

# ============================================================
# ADVANCED TRANSFORMATIONS
# ============================================================
print("Applying advanced transformations...")

# Step 1: Basic timestamp conversions
df2 = df \
    .withColumn("pickup_datetime", to_timestamp(col("lpep_pickup_datetime"))) \
    .withColumn("dropoff_datetime", to_timestamp(col("lpep_dropoff_datetime")))

# Step 2: Time-based feature extraction
df2 = df2 \
    .withColumn("trip_hour", hour(col("lpep_pickup_datetime"))) \
    .withColumn("trip_weekday", dayofweek(col("lpep_pickup_datetime"))) \
    .withColumn("trip_month", month(col("lpep_pickup_datetime"))) \
    .withColumn("trip_day", dayofmonth(col("lpep_pickup_datetime")))

# Step 3: Trip duration calculation
df2 = df2 \
    .withColumn("trip_duration_min",
                (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60)

# Step 4: Time of day categorization (Morning/Afternoon/Evening/Night)
df2 = df2 \
    .withColumn("time_of_day",
        when((col("trip_hour") >= 6) & (col("trip_hour") < 12), "Morning")
        .when((col("trip_hour") >= 12) & (col("trip_hour") < 17), "Afternoon")
        .when((col("trip_hour") >= 17) & (col("trip_hour") < 21), "Evening")
        .otherwise("Night"))

# Step 5: Weekend/Weekday classification
df2 = df2 \
    .withColumn("is_weekend",
        when(col("trip_weekday").isin(1, 7), True).otherwise(False)) \
    .withColumn("day_type",
        when(col("trip_weekday").isin(1, 7), "Weekend").otherwise("Weekday"))

# Step 6: Speed calculation (miles per hour)
df2 = df2 \
    .withColumn("avg_speed_mph",
        when((col("trip_duration_min") > 0) & (col("trip_distance") > 0),
             spark_round(col("trip_distance") / (col("trip_duration_min") / 60), 2))
        .otherwise(None))

# Step 7: Fare efficiency metrics
df2 = df2 \
    .withColumn("fare_per_mile",
        when(col("trip_distance") > 0,
             spark_round(col("total_amount") / col("trip_distance"), 2))
        .otherwise(None)) \
    .withColumn("fare_per_minute",
        when(col("trip_duration_min") > 0,
             spark_round(col("total_amount") / col("trip_duration_min"), 2))
        .otherwise(None))

# Step 8: Trip distance categorization
df2 = df2 \
    .withColumn("distance_category",
        when(col("trip_distance") <= 1, "Short (<1 mi)")
        .when((col("trip_distance") > 1) & (col("trip_distance") <= 5), "Medium (1-5 mi)")
        .when((col("trip_distance") > 5) & (col("trip_distance") <= 10), "Long (5-10 mi)")
        .otherwise("Very Long (>10 mi)"))

# Step 9: Fare amount categorization
df2 = df2 \
    .withColumn("fare_category",
        when(col("total_amount") <= 10, "Budget (<$10)")
        .when((col("total_amount") > 10) & (col("total_amount") <= 25), "Standard ($10-25)")
        .when((col("total_amount") > 25) & (col("total_amount") <= 50), "Premium ($25-50)")
        .otherwise("Luxury (>$50)"))

# Step 10: Tip percentage calculation
df2 = df2 \
    .withColumn("tip_percentage",
        when((col("total_amount") > 0) & (col("tip_amount") > 0),
             spark_round((col("tip_amount") / (col("total_amount") - col("tip_amount"))) * 100, 2))
        .otherwise(0))

# Step 11: Payment type name mapping
df2 = df2 \
    .withColumn("payment_type_name",
        when(col("payment_type") == 1, "Credit Card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No Charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 5, "Unknown")
        .otherwise("Other"))

# Step 12: Vendor name mapping
df2 = df2 \
    .withColumn("vendor_name",
        when(col("VendorID") == 1, "Creative Mobile Technologies")
        .when(col("VendorID") == 2, "VeriFone Inc.")
        .otherwise("Unknown"))

# ============================================================
# DATA QUALITY FILTERING
# ============================================================
print("Applying data quality filters...")

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

print(f"Records after quality filtering: {df_clean.count()}")

# Select analytics columns for fact table
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

print("Loading data into PostgreSQL...")
fact_trips.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", "fact_trips") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Data successfully saved to PostgreSQL.")
spark.stop()
