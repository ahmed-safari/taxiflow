from pyspark.sql import SparkSession
import os

# ---------- LOAD CREDENTIALS FROM ENVIRONMENT ----------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# ---------- CONFIG ----------
NYC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet"
LOCAL_FILE = "/tmp/green_tripdata_2024-01.parquet"
MINIO_PATH = "s3a://datalake/nyc_green/raw/green_tripdata_2024-01.parquet"

# ---------- SPARK SESSION ----------
spark = SparkSession.builder \
    .appName("NYC_GreenTaxi_Extract") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Downloading parquet file...")
os.system(f"wget -O {LOCAL_FILE} {NYC_URL}")

print("Reading downloaded file...")
df = spark.read.parquet(LOCAL_FILE)

print("Writing to MinIO Data Lake...")
df.write.mode("overwrite").parquet(MINIO_PATH)

print("Upload complete.")
spark.stop()
