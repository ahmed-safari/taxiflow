# NYC Green Taxi Data Pipeline Project

## 📋 Project Overview

This project implements an **end-to-end batch data pipeline** for processing NYC Green Taxi trip data. The pipeline extracts raw data from the NYC Taxi & Limousine Commission (TLC), stores it in a data lake (MinIO), transforms the data using Apache Spark, loads it into a PostgreSQL data warehouse, and visualizes insights through Metabase dashboards.

### Architecture Diagram

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   Data Source   │      │    Data Lake    │      │ Data Warehouse  │      │   Dashboard     │
│  (NYC TLC API)  │─────▶│    (MinIO)      │─────▶│  (PostgreSQL)   │─────▶│   (Metabase)    │
└─────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────┘
        │                        │                        │
        │                        │                        │
        └────────────────────────┴────────────────────────┘
                                 │
                         ┌───────┴───────┐
                         │ Apache Spark  │
                         │   Cluster     │
                         └───────────────┘
                                 │
                         ┌───────┴───────┐
                         │   Airflow     │
                         │ Orchestration │
                         └───────────────┘
```

## 🎯 Problem Statement

### Real-World Context

New York City's taxi industry serves over **200 million passengers annually**, making it one of the largest urban transportation networks in the world. The NYC Taxi & Limousine Commission (TLC) collects massive amounts of trip data that, when properly analyzed, can address critical urban mobility challenges:

- **Fleet Optimization:** Taxi companies struggle to position vehicles efficiently, leading to long passenger wait times in high-demand areas while drivers idle in low-demand zones. Understanding hourly and daily trip patterns enables smarter fleet allocation.

- **Revenue Management:** With the rise of ride-sharing apps (Uber, Lyft), traditional taxi services have seen declining revenues. Analyzing payment preferences, trip distances, and fare patterns helps operators develop competitive pricing strategies.

- **Urban Planning:** City planners need data-driven insights to improve infrastructure, identify congestion hotspots, and plan for future transportation needs. Peak-hour analysis informs decisions on traffic signal timing, dedicated taxi lanes, and pickup/dropoff zones.

- **Driver Earnings:** Taxi drivers often work long hours with inconsistent earnings. Data analysis can reveal the most profitable hours, locations, and routes, helping drivers maximize their income.

### Project Objectives

This project builds a scalable data pipeline to transform raw NYC Green Taxi trip records into actionable business intelligence, enabling stakeholders to:

1. **Extract** raw taxi trip data from the NYC TLC public dataset
2. **Store** raw data in a scalable data lake for archival and reprocessing
3. **Transform** the data by cleaning and engineering analytics-ready features
4. **Load** processed data into a data warehouse for efficient querying
5. **Visualize** key metrics through an interactive dashboard

### Key Business Questions Addressed

- _When are the peak demand hours?_ → Optimize driver shift scheduling
- _What payment methods do riders prefer?_ → Inform payment system investments
- _How does trip volume vary by day of week?_ → Plan weekend vs. weekday operations
- _What is the average trip duration by time of day?_ → Set realistic ETAs for passengers

## 🛠️ Technologies Used

| Technology                  | Purpose                   | Why Chosen                                                                         |
| --------------------------- | ------------------------- | ---------------------------------------------------------------------------------- |
| **Docker & Docker Compose** | Containerization          | Enables reproducible, portable deployment of all services                          |
| **Apache Airflow**          | Workflow Orchestration    | Industry-standard tool for scheduling and monitoring data pipelines                |
| **Apache Spark**            | Data Processing           | Distributed processing engine capable of handling large-scale data transformations |
| **MinIO**                   | Data Lake (S3-compatible) | Lightweight, self-hosted object storage compatible with S3 APIs                    |
| **PostgreSQL**              | Data Warehouse            | Robust relational database with excellent analytics capabilities                   |
| **Metabase**                | Business Intelligence     | Open-source BI tool with intuitive interface for creating dashboards               |
| **Redis**                   | Message Broker            | Required by Airflow's Celery Executor for task queuing                             |

## 📁 Project Structure

```
.
├── docker-compose.yaml          # Docker services configuration
├── Dockerfile.spark             # Custom Spark image with required JARs
├── .env                         # Environment variables for Airflow
├── README.md                    # This file
│
├── airflow/
│   ├── dags/
│   │   └── nyc_taxi_etl_dag.py # Airflow DAG definition
│   ├── logs/                    # Airflow logs
│   ├── config/                  # Airflow configuration
│   └── plugins/                 # Airflow plugins
│
├── spark-scripts/
│   ├── extract_to_minio.py     # Extract data from source to MinIO
│   └── transform_load_postgres.py # Transform and load to PostgreSQL
│
├── sql/
│   ├── create_tables.sql       # Schema and table definitions
│   └── analytical_views.sql    # Pre-built analytical views
│
├── minio-data/                  # MinIO data persistence
├── metabase-data/               # Metabase configuration persistence
└── postgres-data/               # PostgreSQL data persistence (auto-created)
```

## 🔄 Data Pipeline Flow

### Pipeline Type: **Batch Processing**

The pipeline runs on a daily schedule (configurable) and consists of two main stages:

### Stage 1: Extract to Data Lake (`extract_to_minio.py`)

1. Downloads NYC Green Taxi parquet data from the TLC website
2. Reads the data into a Spark DataFrame
3. Writes the raw data to MinIO data lake at `s3a://datalake/nyc_green/raw/`

### Stage 2: Transform and Load (`transform_load_postgres.py`)

1. Reads raw parquet data from MinIO
2. Performs transformations:
   - Converts timestamps to proper datetime format
   - Extracts `trip_hour` from pickup time
   - Extracts `trip_weekday` from pickup time
   - Calculates `trip_duration_min` (dropoff - pickup time)
3. Selects relevant columns for analytics
4. Loads the transformed data into PostgreSQL `fact_trips` table

### Airflow DAG

```
extract_to_minio >> transform_load_postgres
```

The DAG runs daily and orchestrates both Spark jobs in sequence.

## 📊 Data Model

### Fact Table: `fact_trips`

| Column            | Type      | Description                             |
| ----------------- | --------- | --------------------------------------- |
| trip_id           | SERIAL    | Primary key                             |
| vendor_id         | INT       | Taxi vendor identifier                  |
| pickup_datetime   | TIMESTAMP | Trip start time                         |
| dropoff_datetime  | TIMESTAMP | Trip end time                           |
| trip_distance     | FLOAT     | Distance traveled in miles              |
| total_amount      | FLOAT     | Total fare amount                       |
| payment_type      | INT       | Payment method (FK to dim_payment_type) |
| trip_hour         | INT       | Hour of pickup (0-23)                   |
| trip_weekday      | INT       | Day of week (1=Sunday, 7=Saturday)      |
| trip_duration_min | FLOAT     | Trip duration in minutes                |

### Dimension Table: `dim_payment_type`

| payment_type | payment_desc |
| ------------ | ------------ |
| 1            | Credit card  |
| 2            | Cash         |
| 3            | No charge    |
| 4            | Dispute      |
| 5            | Unknown      |
| 6            | Voided trip  |

### Pre-built Analytical Views

- `trips_by_hour` - Trip count distribution by hour of day
- `trips_by_weekday` - Trip count distribution by day of week
- `payment_type_distribution` - Trip count by payment method
- `avg_trip_duration_hour` - Average trip duration by hour

## 🚀 Setup & Installation

### Prerequisites

- **Docker Desktop** (v20.10+) with at least **4GB RAM** allocated
- **Docker Compose** (v2.0+)
- **Git** (optional, for cloning)

### Step 1: Clone/Download the Project

```bash
cd /path/to/your/project
```

### Step 2: Create Environment File

Create a `.env` file in the project root with the following variables:

```bash
# Airflow
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.

# MinIO (Data Lake)
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# PostgreSQL (Data Warehouse)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=sampledb
POSTGRES_USER=demo
POSTGRES_PASSWORD=demo
```

| Variable | Description | Default |
|----------|-------------|---------|
| `AIRFLOW_UID` | User ID for Airflow containers | `50000` |
| `AIRFLOW_PROJ_DIR` | Airflow project directory | `.` |
| `MINIO_ENDPOINT` | MinIO S3-compatible API endpoint | `http://minio:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key (username) | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key (password) | `minioadmin` |
| `POSTGRES_HOST` | PostgreSQL hostname | `postgres` |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `POSTGRES_DB` | PostgreSQL database name | `sampledb` |
| `POSTGRES_USER` | PostgreSQL username | `demo` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `demo` |

> ⚠️ **Security Note:** For production deployments, change all default passwords and never commit the `.env` file to version control.

### Step 3: Create Required Directories

```bash
mkdir -p dags logs plugins config minio-data metabase-data
```

### Step 4: Build and Start Services

```bash
# Build the custom Spark image
docker compose build

# Start all services (first run takes ~5-10 minutes)
docker compose up -d
```

### Step 5: Verify Services are Running

```bash
docker compose ps
```

All services should show as "healthy" or "running".

### Step 6: Create MinIO Bucket

1. Open MinIO Console: http://localhost:9001
2. Login with:
   - **Username:** `minioadmin`
   - **Password:** `minioadmin`
3. Create a new bucket named: `datalake`

### Step 7: Initialize PostgreSQL Schema (Optional)

If you want to pre-create the schema with proper constraints:

```bash
# Connect to PostgreSQL container
docker exec -it postgres psql -U demo -d sampledb

# Run the SQL scripts
\i /path/to/sql/create_tables.sql
\i /path/to/sql/analytical_views.sql

# Exit
\q
```

Alternatively, the Spark job will auto-create the `fact_trips` table.

### Step 8: Configure & Run Airflow DAG

1. Open Airflow UI: http://localhost:8080
2. Login with:
   - **Username:** `airflow`
   - **Password:** `airflow`
3. Find the DAG `nyc_taxi_etl` in the list
4. Toggle the DAG to "Active" (unpause it)
5. Trigger a manual run by clicking the "Play" button

### Step 9: Set Up Metabase Dashboard

1. Open Metabase: http://localhost:3000
2. Complete the initial setup wizard
3. Add PostgreSQL as a data source:
   - **Host:** `postgres`
   - **Port:** `5432`
   - **Database:** `sampledb`
   - **Username:** `demo`
   - **Password:** `demo`
4. Create visualizations (see Dashboard section below)

## 🌐 Service URLs & Credentials

| Service             | URL                   | Username           | Password           |
| ------------------- | --------------------- | ------------------ | ------------------ |
| **Airflow**         | http://localhost:8080 | airflow            | airflow            |
| **MinIO Console**   | http://localhost:9001 | minioadmin         | minioadmin         |
| **Spark Master UI** | http://localhost:8080 | -                  | -                  |
| **Metabase**        | http://localhost:3000 | (set during setup) | (set during setup) |
| **PostgreSQL**      | localhost:5432        | demo               | demo               |

> **Note:** Airflow and Spark Master both use port 8080. Access Spark UI at http://localhost:8080 when Airflow is stopped, or modify ports in docker-compose.yaml.

## 📈 Dashboard

The Metabase dashboard includes at least two visualization tiles:

### 1. Trips by Hour of Day (Temporal Distribution)

A bar chart showing the number of trips per hour, revealing peak taxi usage times.

**SQL Query:**

```sql
SELECT trip_hour, COUNT(*) as total_trips
FROM fact_trips
GROUP BY trip_hour
ORDER BY trip_hour;
```

### 2. Payment Type Distribution (Categorical Distribution)

A pie chart showing the breakdown of trips by payment method.

**SQL Query:**

```sql
SELECT
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        ELSE 'Other'
    END as payment_method,
    COUNT(*) as total_trips
FROM fact_trips
GROUP BY payment_type;
```

### Additional Dashboard Ideas

- **Trips by Day of Week** - Bar chart showing weekday vs weekend patterns
- **Average Trip Duration by Hour** - Line chart for operational insights
- **Trip Distance Distribution** - Histogram of trip lengths
- **Revenue by Payment Type** - Stacked bar chart

## 🛑 Stopping the Services

```bash
# Stop all services (preserves data)
docker compose down

# Stop and remove all data volumes
docker compose down -v
```

## 🔧 Troubleshooting

### Common Issues

**1. Airflow webserver won't start**

- Ensure at least 4GB RAM is allocated to Docker
- Check logs: `docker compose logs airflow-webserver`

**2. Spark jobs fail**

- Verify MinIO bucket `datalake` exists
- Check Spark logs: `docker logs spark-master`

**3. Cannot connect to PostgreSQL from Metabase**

- Use `postgres` as hostname (Docker internal DNS)
- Ensure PostgreSQL container is running

**4. MinIO connection issues in Spark**

- Verify MinIO is running: `docker compose ps minio`
- Check network connectivity between containers

### Viewing Logs

```bash
# Airflow logs
docker compose logs -f airflow-scheduler

# Spark Master logs
docker logs -f spark-master

# PostgreSQL logs
docker logs -f postgres
```

## 📝 Dataset Information

- **Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Dataset:** Green Taxi Trip Records (January 2024)
- **Format:** Parquet
- **Size:** ~50MB compressed

## 🔮 Future Improvements

- [ ] Add data quality checks with Great Expectations
- [ ] Implement incremental data loading
- [ ] Add more dimension tables (locations, vendors)
- [ ] Set up alerts for pipeline failures
- [ ] Add data lineage tracking
- [ ] Implement CI/CD for DAG deployments

## 📚 References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [Metabase Documentation](https://www.metabase.com/docs/latest/)
- [NYC TLC Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)

---

**Author:** DSAI5102 Data Architecture & Engineering Project  
**Course:** Fall 2025
