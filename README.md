# Transit Pulse

If you've ever watched a bus app say "3 minutes away" for ten straight minutes, you've already met the problem this project is trying to measure.

Transit Pulse pulls live vehicle GPS from King County Metro / Puget Sound GTFS feeds, compares each update against the published schedule, and computes how late (or early) buses actually are — aggregated into five-minute windows per route. Data is public GTFS; the project is not affiliated with any transit agency.

The stack runs end-to-end on a laptop (Docker + Spark local mode) and has also been exercised on real AWS: S3, Glue, and Redshift Serverless, with Terraform.

## How it works

```
GTFS-RT (live GPS)              GTFS static (schedules)
        │                                  │
        ▼                                  ▼
   Kafka topic                    MinIO or S3 raw-zone
   vehicle-positions              gtfs-static/ + vehicle-positions/
        │                                  │
        ├──────── Spark Streaming ─────────┤
        │   • archive raw events           │
        │   • compute delay windows        │
        │                                  │
        ├──────── Airflow ─────────────────┤
        │   • refresh static data daily    │
        │   • batch backfill by date       │
        │                                  │
        └──────────────┬───────────────────┘
                       ▼
            curated-zone/route-delay-windows/
            (Parquet, partitioned by route + date)
                       │
           ┌───────────┴───────────┐
           ▼                       ▼
      Glue catalog            Redshift / Postgres
      (schema discovery)      route_delay_metrics
```

Locally, MinIO stands in for S3 and a Postgres container on port 5433 stands in for Redshift. On AWS, the same Python jobs talk to real buckets — you mostly change env vars, not business logic.

Streaming and batch share one module (`common/delay_logic.py`) so they can't drift apart silently.

## What a real run produced

Numbers from 2026-07-03, not back-of-napkin estimates:

| What | Result |
|------|--------|
| Vehicle GPS events ingested | 8,803 |
| Matched to schedule (delay computed) | 8,107 — about **92%** |
| Five-minute delay windows written | 327 |
| Routes with data | 86 |
| Join strategy used | `trip_id_nearest` |

Warehouse load for that date landed 87 rows in `route_delay_metrics` (a subset of curated partitions for that date range).

## The join problem (and why it's not hidden)

The OneBusAway feed for this agency usually doesn't send `stop_id` or `current_stop_sequence`. If you join on `trip_id + stop_id` anyway, you throw away most of the data.

So the pipeline checks null rates at startup and picks a strategy:

- **`trip_id + stop_id`** when the feed actually has stop fields.
- **`trip_id_nearest`** when it doesn't — match each GPS ping to whichever scheduled stop on that trip is closest in time, then measure delay from there.

That's a deliberate compromise for a sparse realtime feed. It works well enough to track route-level reliability; individual rows can look wild (a bus "32 minutes early" usually means the nearest stop wasn't the true stop, not a GPS error). Streaming does the lookup with a broadcast UDF; batch uses a SQL window so `stop_times` aren't collected to the driver and OOM.

Set `DELAY_CALCULATOR_JOIN_MODE=auto` (default) to let the job decide, or force a mode if you're debugging.

## Run it locally

You'll want Docker, Python 3.10+, and roughly 8 GB of free RAM — Spark and Airflow are hungry.

```bash
cp .env.example .env
make up
make kafka-topics
make install
source .venv/bin/activate
make run-gtfs-static
```

Then, in separate terminals as needed:

```bash
make run-gtfs-realtime          # poll GTFS-RT → Kafka
make run-delay-calculator       # archive raw + write live delay windows
make run-historical-backfill START_DATE=2026-07-03 END_DATE=2026-07-03
.venv/bin/python scripts/read_curated_delays.py
.venv/bin/python -m warehouse.load_curated --start-date 2026-07-03 --end-date 2026-07-03
```

Handy URLs: Airflow at http://localhost:8081, MinIO console at http://localhost:9001. `make test` runs the unit tests. `make down` stops everything.

The realtime producer uses OneBusAway's public `TEST` API key out of the box (`GTFS_REALTIME_API_KEY` in `.env`). A dedicated key takes ~20 business days to get approved — swap it in for production use.

Use `.venv/bin/python`, not system Python — PySpark won't be there otherwise.

## Deploy to AWS

`terraform/` provisions S3 buckets, a Glue database + crawler, least-privilege IAM roles, and optionally Redshift Serverless. The Python in `ingestion/`, `streaming/`, `batch/`, and `warehouse/` doesn't need rewrites — point it at real endpoints and drop the MinIO credentials so IAM takes over.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # pick globally unique bucket names
terraform init && terraform plan && terraform apply
```

After that:

1. Upload curated Parquet under `route-delay-windows/` in the curated bucket.
2. Run the Glue crawler: `aws glue start-crawler --name transit-pulse-curated-route-delay-windows`
3. Apply `warehouse/redshift_schema.sql` if Redshift is up.
4. Load with `warehouse/load_curated.py` (`WAREHOUSE_BACKEND=redshift`, `REDSHIFT_COPY_IAM_ROLE` from `terraform output`).

Redshift Serverless bills while the workgroup exists. Tear it down when not in use and keep the cheaper resources:

```bash
terraform destroy -target=aws_redshiftserverless_workgroup.main \
                  -target=aws_redshiftserverless_namespace.main
```

S3 + Glue + IAM can stay up for a few cents. `terraform plan` will show Redshift as "2 to add" until you re-apply — that's expected after a targeted destroy, not drift.

Env var cheat sheet: `terraform/outputs.tf` → `env_snippet`.

## What the output looks like

### Local MinIO (`scripts/read_curated_delays.py`)

Verified run against `s3a://curated-zone/route-delay-windows/` on **2026-07-03**:

```
Total rows: 162
Distinct routes: 86
```

Parquet rows also carry `window_start` and `window_end` (five-minute buckets). The sample rows below come from the **AWS** query on the same curated dataset after upload to S3.

### Curated Parquet / Glue catalog (verified on AWS, 2026-07-04)

Rows from curated Parquet in S3, queried via Redshift over the Glue external table `awsdatacatalog.transit_pulse_curated.route_delay_windows`:

```
route_id   avg_delay_seconds   vehicle_event_count   window_date
100269     -9.25               12                    2026-07-04
100018     -624.5              10                    2026-07-04
100269      3.0                66                    2026-07-04
100018     -443.8               27                    2026-07-04
```

(`window_start` / `window_end` are stored in the Parquet files; this query selected `route_id`, `avg_delay_seconds`, `vehicle_event_count`, and `window_date` only.)

### Redshift query (verified 2026-07-04)

```sql
SELECT route_id, avg_delay_seconds, vehicle_event_count, window_date
FROM awsdatacatalog.transit_pulse_curated.route_delay_windows
ORDER BY window_date DESC
LIMIT 10;
```

```
 route_id | avg_delay_seconds | vehicle_event_count | window_date
----------+-------------------+---------------------+-------------
 100269   |             -9.25 |                  12 | 2026-07-04
 100018   |            -624.5 |                  10 | 2026-07-04
 100269   |               3.0 |                  66 | 2026-07-04
 100018   |            -443.8 |                  27 | 2026-07-04
 100269   |             -0.02 |                  54 | 2026-07-04
 100018   |            -314.2 |                  13 | 2026-07-04
 100269   |              0.45 |                  55 | 2026-07-04
 100018   |            -233.1 |                  20 | 2026-07-04
 100269   |              -3.0 |                  48 | 2026-07-04
 100018   |            -78.05 |                  20 | 2026-07-04
```

Executed via `aws redshift-data execute-statement` against workgroup `transit-pulse-portfolio` on 2026-07-04. Negative values mean early vs schedule — expected with the `trip_id_nearest` join on a sparse feed.

Local warehouse loads use the same logical schema in `route_delay_metrics` (`vehicle_event_count` renamed to `vehicle_count` on insert); see `warehouse/load_curated.py`.

## Known limitations

**Memory on a laptop.** Spark local mode + Airflow backfill can OOM at 8 GB. Driver memory is capped, batch uses a SQL nearest-join (no giant driver collect), and Postgres is mapped to host port 5433 to avoid conflicting with a local 5432. Redshift at 8 RPU can also choke on big scans — load by date range and tear down when idle.

**MSK and EMR.** IAM policies are written for them (`terraform/iam.tf`), but managed Kafka and Spark clusters are not deployed — a deliberate cost decision, documented in Terraform.

**Streaming in the cloud.** Structured Streaming with Kafka checkpoints runs locally. Batch backfill over S3 archives is the path that scales on AWS without redesigning the job.

**Feed quality.** `trip_id_nearest` is only as good as the GPS update rate. Sparse routes produce noisy outliers; treat large delay values skeptically unless `stop_id` coverage improves.

## Repo map

```
ingestion/          GTFS static loader + realtime Kafka producer
streaming/          Spark streaming: archiver + delay calculator
batch/              Historical backfill over archived raw Parquet
common/             Shared join/delay logic (streaming + batch)
orchestration/dags/ Airflow: static refresh + backfill
warehouse/          Redshift schema + loader into Postgres or Redshift
terraform/          S3, Glue, IAM, Redshift Serverless
tests/
```

## Before you push

`.gitignore` keeps out `.env`, `terraform/.terraform/`, `terraform.tfstate*`, and `terraform/terraform.tfvars`. Commit `terraform.tfvars.example`, not account-specific bucket names.

Quick leak check:

```bash
git grep -iE 'AKIA[0-9A-Z]{16}|aws_secret_access_key\s*=\s*["\'][^"\']+["\']' -- ':!.env.example'
```

No AWS keys should turn up. `minioadmin` only appears in `.env.example` and Docker Compose defaults.

---

Public GTFS data from King County Metro; realtime feed via OneBusAway. Not an official transit product.
