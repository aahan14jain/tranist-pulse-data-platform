# Transit Pulse

Transit Pulse is a personal portfolio project that ingests and analyzes real public transit data from King County Metro and Sound Transit (Puget Sound region) to explore vehicle reliability and on-time performance. It uses publicly available GTFS static and GTFS Realtime feeds. This project is not affiliated with, endorsed by, or operated on behalf of any transit agency.

The GTFS-realtime vehicle-positions feed is served by OneBusAway. Local development uses OneBusAway's public `TEST` API key (`GTFS_REALTIME_API_KEY` in `.env`) because dedicated keys require a ~20 business-day approval window; a production deployment would use a dedicated key instead. That is an intentional documented tradeoff, not a missing piece of the pipeline.

## Local Setup

1. Copy `.env.example` to `.env` and review the placeholder values.
2. Start the local stack (Zookeeper, Kafka, MinIO, Postgres, Airflow):

   ```bash
   make up
   ```

3. Create the Kafka topic used for vehicle position events:

   ```bash
   make kafka-topics
   ```

4. Create a virtualenv and install Python dependencies:

   ```bash
   make install
   source .venv/bin/activate
   ```

   Airflow runs in Docker (see step 2). For a local Airflow CLI install, use
   `requirements-airflow.txt` on Python 3.8–3.12 only.

5. Run tests:

   ```bash
   make test
   ```

6. Load GTFS static reference data into MinIO:

   ```bash
   make run-gtfs-static
   ```

Useful commands: `make down` stops all services; `make logs` tails container logs. Airflow UI: http://localhost:8081 by default (`AIRFLOW_WEBSERVER_PORT` in `.env`; default credentials set during `airflow-init`).
