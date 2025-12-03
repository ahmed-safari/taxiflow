from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, dayofweek
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

print("Transforming data...")

df2 = df \
    .withColumn("pickup_datetime", to_timestamp(col("lpep_pickup_datetime"))) \
    .withColumn("dropoff_datetime", to_timestamp(col("lpep_dropoff_datetime"))) \
    .withColumn("trip_hour", hour(col("lpep_pickup_datetime"))) \
    .withColumn("trip_weekday", dayofweek(col("lpep_pickup_datetime"))) \
    .withColumn("trip_duration_min",
                (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60)

# Select analytics columns
fact_trips = df2.select(
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance",
    "total_amount",
    "payment_type",
    "trip_hour",
    "trip_weekday",
    "trip_duration_min",
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
