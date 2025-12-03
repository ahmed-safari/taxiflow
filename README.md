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
- _What are the traffic patterns throughout the day?_ → Speed analysis by hour reveals congestion
- _How do tip percentages vary by payment type?_ → Credit card users tip 17.57% vs 0% for cash
- _What are the most profitable trip segments?_ → Fare per mile/minute analysis
- _How do the taxi vendors compare?_ → Vendor performance metrics

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
├── Makefile                     # Quick commands for pipeline management
├── .env                         # Environment variables (create from .env.example)
├── .env.example                 # Environment variables template
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
│   ├── init.sql                # Auto-initialization script (runs on startup)
│   ├── create_tables.sql       # Schema and table definitions
│   └── analytical_views.sql    # Pre-built analytical views
│
├── screenshots/                 # Dashboard and UI screenshots
│   └── (add your screenshots here)
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
2. Performs **12 advanced transformations**:
   - **Time Features:** Extracts `trip_hour`, `trip_weekday`, `trip_month`, `trip_day`
   - **Time of Day:** Categorizes trips into Morning/Afternoon/Evening/Night
   - **Day Type:** Classifies as Weekend or Weekday
   - **Trip Duration:** Calculates duration in minutes from timestamps
   - **Speed Analysis:** Computes `avg_speed_mph` (distance ÷ time)
   - **Fare Efficiency:** Calculates `fare_per_mile` and `fare_per_minute`
   - **Distance Categories:** Short (<1mi) / Medium (1-5mi) / Long (5-10mi) / Very Long (>10mi)
   - **Fare Categories:** Budget (<$10) / Standard ($10-25) / Premium ($25-50) / Luxury (>$50)
   - **Tip Analysis:** Calculates `tip_percentage` from fare and tip amounts
   - **Payment Mapping:** Converts payment codes to names (Credit Card, Cash, etc.)
   - **Vendor Mapping:** Maps vendor IDs to company names
3. Applies **data quality filters** (removes invalid fares, distances, durations, speeds)
4. Loads the transformed data into PostgreSQL `fact_trips` table

### Airflow DAG

```
extract_to_minio >> transform_load_postgres
```

The DAG runs daily and orchestrates both Spark jobs in sequence.

## 📊 Data Model

### Fact Table: `fact_trips`

The fact table contains **23 columns** with both raw and derived analytics features:

| Column            | Type      | Description                                   |
| ----------------- | --------- | --------------------------------------------- |
| VendorID          | INT       | Taxi vendor identifier                        |
| vendor_name       | TEXT      | Vendor company name (derived)                 |
| pickup_datetime   | TIMESTAMP | Trip start time                               |
| dropoff_datetime  | TIMESTAMP | Trip end time                                 |
| trip_distance     | FLOAT     | Distance traveled in miles                    |
| total_amount      | FLOAT     | Total fare amount                             |
| tip_amount        | FLOAT     | Tip amount                                    |
| payment_type      | INT       | Payment method code                           |
| payment_type_name | TEXT      | Payment method name (Credit Card, Cash, etc.) |
| trip_hour         | INT       | Hour of pickup (0-23)                         |
| trip_weekday      | INT       | Day of week (1=Sunday, 7=Saturday)            |
| trip_month        | INT       | Month of pickup                               |
| trip_day          | INT       | Day of month                                  |
| trip_duration_min | FLOAT     | Trip duration in minutes                      |
| time_of_day       | TEXT      | Morning / Afternoon / Evening / Night         |
| is_weekend        | BOOLEAN   | True if Saturday or Sunday                    |
| day_type          | TEXT      | "Weekend" or "Weekday"                        |
| avg_speed_mph     | FLOAT     | Average speed in miles per hour               |
| fare_per_mile     | FLOAT     | Fare efficiency ($/mile)                      |
| fare_per_minute   | FLOAT     | Fare efficiency ($/minute)                    |
| distance_category | TEXT      | Short/Medium/Long/Very Long                   |
| fare_category     | TEXT      | Budget/Standard/Premium/Luxury                |
| tip_percentage    | FLOAT     | Tip as percentage of fare                     |

### Pre-built Analytical Views

The `nyc_taxi` schema contains **11 pre-aggregated views** for dashboard creation:

| View Name                      | Description                              | Use Case                       |
| ------------------------------ | ---------------------------------------- | ------------------------------ |
| `v_trips_by_time_of_day`       | Trips by Morning/Afternoon/Evening/Night | Pie chart of demand patterns   |
| `v_weekend_weekday_comparison` | Weekend vs Weekday metrics               | Comparison bar chart           |
| `v_distance_category_dist`     | Trip distance distribution               | Pie chart of trip lengths      |
| `v_fare_category_breakdown`    | Fare tier analysis                       | Revenue segmentation           |
| `v_avg_speed_by_hour`          | Traffic speed patterns                   | Line chart of congestion       |
| `v_tip_by_payment`             | Tips by payment method                   | Bar chart of tipping behavior  |
| `v_vendor_performance`         | Vendor comparison metrics                | Vendor analysis table          |
| `v_fare_efficiency`            | Fare per mile/minute by time             | Efficiency analysis            |
| `v_hourly_revenue`             | Revenue by hour                          | Area chart of revenue patterns |
| `v_daily_summary`              | Daily operational metrics                | Time-series dashboard          |
| `v_kpi_summary`                | Executive KPIs                           | Summary number cards           |

## 🚀 Setup & Installation

### Quick Start (TL;DR)

```bash
# 1. Setup environment
cp .env.example .env

# 2. Build and start all services
make build
make up

# 3. Create MinIO bucket (wait ~30 seconds for services to start)
make init-minio

# 4. Run the ETL pipeline
make run-pipeline

# 5. Open Metabase and create dashboards
open http://localhost:3000
```

### Available Make Commands

| Command             | Description                 |
| ------------------- | --------------------------- |
| `make help`         | Show all available commands |
| `make build`        | Build Docker images         |
| `make up`           | Start all services          |
| `make down`         | Stop all services           |
| `make restart`      | Restart all services        |
| `make clean`        | Stop and remove all data    |
| `make init-minio`   | Create the datalake bucket  |
| `make run-pipeline` | Run the full ETL pipeline   |
| `make status`       | Show service health status  |
| `make logs`         | Tail logs from all services |
| `make db-shell`     | Connect to PostgreSQL CLI   |

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

| Variable            | Description                      | Default             |
| ------------------- | -------------------------------- | ------------------- |
| `AIRFLOW_UID`       | User ID for Airflow containers   | `50000`             |
| `AIRFLOW_PROJ_DIR`  | Airflow project directory        | `.`                 |
| `MINIO_ENDPOINT`    | MinIO S3-compatible API endpoint | `http://minio:9000` |
| `MINIO_ACCESS_KEY`  | MinIO access key (username)      | `minioadmin`        |
| `MINIO_SECRET_KEY`  | MinIO secret key (password)      | `minioadmin`        |
| `POSTGRES_HOST`     | PostgreSQL hostname              | `postgres`          |
| `POSTGRES_PORT`     | PostgreSQL port                  | `5432`              |
| `POSTGRES_DB`       | PostgreSQL database name         | `sampledb`          |
| `POSTGRES_USER`     | PostgreSQL username              | `demo`              |
| `POSTGRES_PASSWORD` | PostgreSQL password              | `demo`              |

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

1. Open Airflow UI: http://localhost:8081
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
| **Airflow**         | http://localhost:8081 | airflow            | airflow            |
| **MinIO Console**   | http://localhost:9001 | minioadmin         | minioadmin         |
| **Spark Master UI** | http://localhost:8080 | -                  | -                  |
| **Metabase**        | http://localhost:3000 | (set during setup) | (set during setup) |
| **PostgreSQL**      | localhost:5432        | demo               | demo               |

## 📈 Dashboard

The Metabase dashboard provides visual insights into NYC Green Taxi trip patterns.

### Dashboard Screenshots

> 📸 **Add your dashboard screenshots to the `screenshots/` folder**

After creating your Metabase dashboard, save screenshots with these recommended names:

- `dashboard_overview.png` - Full dashboard view
- `trips_by_time_of_day.png` - Time of day distribution
- `distance_categories.png` - Distance category breakdown
- `speed_by_hour.png` - Traffic speed analysis
- `payment_tips.png` - Tips by payment type

### Recommended Dashboard Visualizations

Create these charts in Metabase using **+ New → Question → Native Query**:

#### Tile 1: Trips by Time of Day (Pie Chart)

```sql
SELECT time_of_day, trip_count
FROM nyc_taxi.v_trips_by_time_of_day;
```

#### Tile 2: Weekend vs Weekday Revenue (Bar Chart)

```sql
SELECT day_type, total_revenue, trip_count
FROM nyc_taxi.v_weekend_weekday_comparison;
```

#### Tile 3: Distance Category Distribution (Pie Chart)

```sql
SELECT distance_category, percentage
FROM nyc_taxi.v_distance_category_dist;
```

#### Tile 4: Fare Category Breakdown (Pie Chart)

```sql
SELECT fare_category, percentage
FROM nyc_taxi.v_fare_category_breakdown;
```

#### Tile 5: Traffic Speed by Hour (Line Chart)

```sql
SELECT trip_hour, avg_speed_mph
FROM nyc_taxi.v_avg_speed_by_hour;
```

#### Tile 6: Tips by Payment Type (Bar Chart)

```sql
SELECT payment_type_name, avg_tip_pct, total_tips
FROM nyc_taxi.v_tip_by_payment
WHERE payment_type_name != 'Unknown';
```

#### Tile 7: Hourly Revenue Pattern (Area Chart)

```sql
SELECT trip_hour, total_revenue, trip_count
FROM nyc_taxi.v_hourly_revenue;
```

#### Tile 8: Vendor Performance (Table)

```sql
SELECT * FROM nyc_taxi.v_vendor_performance;
```

#### Tile 9: KPI Summary Cards

```sql
SELECT * FROM nyc_taxi.v_kpi_summary;
```

## 🛑 Stopping the Services

```bash
# Using Makefile (recommended)
make down          # Stop all services (preserves data)
make clean         # Stop and remove all data (with confirmation)

# Or using docker compose directly
docker compose down        # Stop all services (preserves data)
docker compose down -v     # Stop and remove all data volumes
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
- **Records:** 56,551 raw → 55,852 after quality filtering
- **Revenue:** $1,252,467.49 total
- **Tips:** $126,820.52 total

## 🔮 Future Improvements

- [ ] Add data quality checks with Great Expectations
- [ ] Implement incremental data loading
- [x] ~~Add more dimension tables (locations, vendors)~~ Added vendor and payment type names
- [x] ~~Add derived analytics features~~ Added 12 advanced transformations
- [x] ~~Create pre-built analytical views~~ Added 11 views in nyc_taxi schema
- [ ] Set up alerts for pipeline failures
- [ ] Add data lineage tracking
- [ ] Implement CI/CD for DAG deployments
- [ ] Add unit tests for Spark transformations
- [ ] Implement partitioning for large datasets

## 📚 References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [Metabase Documentation](https://www.metabase.com/docs/latest/)
- [NYC TLC Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)

---

**Author:** DSAI5102 Data Architecture & Engineering Project  
**Course:** Fall 2025
