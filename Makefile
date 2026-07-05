.PHONY: up down logs install test kafka-topics run-gtfs-static run-gtfs-realtime run-delay-calculator run-historical-backfill

COMPOSE := docker compose
KAFKA_CONTAINER := transit-pulse-kafka
VEHICLE_POSITIONS_TOPIC := vehicle-positions
VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

install:
	python3 -m venv $(VENV)
	$(PIP) install -r requirements.txt

test: install
	$(PYTHON) -m pytest

kafka-topics:
	docker exec $(KAFKA_CONTAINER) kafka-topics \
		--bootstrap-server localhost:9092 \
		--create \
		--if-not-exists \
		--topic $(VEHICLE_POSITIONS_TOPIC) \
		--partitions 3 \
		--replication-factor 1

run-gtfs-static: install
	$(PYTHON) -m ingestion.gtfs_static_loader

run-gtfs-realtime: install
	$(PYTHON) -m ingestion.gtfs_realtime_producer

run-delay-calculator: install
	$(PYTHON) -m streaming.delay_calculator_job

# Usage: make run-historical-backfill START_DATE=2026-07-01 END_DATE=2026-07-01
run-historical-backfill: install
	$(PYTHON) -m batch.historical_backfill --start-date $(START_DATE) --end-date $(END_DATE)
