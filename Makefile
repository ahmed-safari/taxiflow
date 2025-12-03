# ============================================
# NYC Green Taxi Data Pipeline - Makefile
# ============================================
# Quick commands to manage the pipeline
# Usage: make <command>
# ============================================

.PHONY: help build up down restart logs clean init-minio run-pipeline status ps

# Default target
help:
	@echo "NYC Green Taxi Data Pipeline - Available Commands"
	@echo "=================================================="
	@echo ""
	@echo "Setup & Lifecycle:"
	@echo "  make build        - Build Docker images"
	@echo "  make up           - Start all services"
	@echo "  make down         - Stop all services"
	@echo "  make restart      - Restart all services"
	@echo "  make clean        - Stop services and remove all data volumes"
	@echo ""
	@echo "Operations:"
	@echo "  make init-minio   - Create the datalake bucket in MinIO"
	@echo "  make run-pipeline - Manually trigger the ETL pipeline"
	@echo ""
	@echo "Monitoring:"
	@echo "  make status       - Show service health status"
	@echo "  make ps           - Show running containers"
	@echo "  make logs         - Tail logs from all services"
	@echo "  make logs-spark   - Tail Spark master logs"
	@echo "  make logs-airflow - Tail Airflow scheduler logs"
	@echo ""
	@echo "Access URLs:"
	@echo "  Airflow:    http://localhost:8081  (airflow/airflow)"
	@echo "  Spark UI:   http://localhost:8080"
	@echo "  MinIO:      http://localhost:9001  (minioadmin/minioadmin)"
	@echo "  Metabase:   http://localhost:3000"
	@echo "  PostgreSQL: localhost:5432         (demo/demo)"

# ============================================
# SETUP & LIFECYCLE
# ============================================

# Build all Docker images
build:
	@echo "🔨 Building Docker images..."
	docker compose build

# Start all services
up:
	@echo "🚀 Starting all services..."
	docker compose up -d
	@echo ""
	@echo "✅ Services started! Access URLs:"
	@echo "   Airflow:    http://localhost:8081"
	@echo "   Spark UI:   http://localhost:8080"
	@echo "   MinIO:      http://localhost:9001"
	@echo "   Metabase:   http://localhost:3000"

# Stop all services
down:
	@echo "🛑 Stopping all services..."
	docker compose down

# Restart all services
restart: down up

# Stop and remove all data (DANGEROUS!)
clean:
	@echo "⚠️  WARNING: This will delete all data!"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo "🧹 Cleaning up..."
	docker compose down -v
	rm -rf postgres-data minio-data metabase-data logs/*
	@echo "✅ Cleanup complete"

# ============================================
# OPERATIONS
# ============================================

# Create MinIO bucket for data lake
init-minio:
	@echo "🪣 Creating MinIO datalake bucket..."
	@docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
	@docker exec minio mc mb local/datalake --ignore-existing 2>/dev/null || \
		(echo "⚠️  Could not create bucket via mc. Please create 'datalake' bucket manually at http://localhost:9001")
	@echo "✅ MinIO bucket ready"

# Manually run the ETL pipeline
run-pipeline:
	@echo "🔄 Running ETL Pipeline..."
	@echo ""
	@echo "Step 1/2: Extracting data to MinIO..."
	docker exec spark-master spark-submit /opt/spark/work-dir/extract_to_minio.py
	@echo ""
	@echo "Step 2/2: Transforming and loading to PostgreSQL..."
	docker exec spark-master spark-submit /opt/spark/work-dir/transform_load_postgres.py
	@echo ""
	@echo "✅ Pipeline complete!"

# Run only the extract step
run-extract:
	@echo "📥 Running extract to MinIO..."
	docker exec spark-master spark-submit /opt/spark/work-dir/extract_to_minio.py

# Run only the transform/load step
run-transform:
	@echo "🔄 Running transform and load to PostgreSQL..."
	docker exec spark-master spark-submit /opt/spark/work-dir/transform_load_postgres.py

# ============================================
# MONITORING
# ============================================

# Show container status
status:
	@echo "📊 Service Status:"
	@echo ""
	docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# Alias for status
ps: status

# Tail all logs
logs:
	docker compose logs -f

# Tail Spark master logs
logs-spark:
	docker logs -f spark-master

# Tail Airflow scheduler logs
logs-airflow:
	docker compose logs -f airflow-scheduler

# Tail PostgreSQL logs
logs-postgres:
	docker logs -f postgres

# ============================================
# DATABASE UTILITIES
# ============================================

# Connect to PostgreSQL CLI
db-shell:
	@echo "🐘 Connecting to PostgreSQL..."
	docker exec -it postgres psql -U demo -d sampledb

# Run a SQL query
db-query:
	@read -p "Enter SQL query: " query && \
	docker exec postgres psql -U demo -d sampledb -c "$$query"

# Show fact_trips row count
db-count:
	@echo "📊 Fact table row count:"
	@docker exec postgres psql -U demo -d sampledb -t -c "SELECT COUNT(*) FROM fact_trips;" 2>/dev/null || \
		echo "Table does not exist yet. Run the pipeline first."

# ============================================
# DEVELOPMENT
# ============================================

# Copy environment template
env-setup:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✅ Created .env from .env.example"; \
	else \
		echo "⚠️  .env already exists"; \
	fi

# Validate docker-compose configuration
validate:
	@echo "🔍 Validating docker-compose configuration..."
	docker compose config --quiet && echo "✅ Configuration is valid"
