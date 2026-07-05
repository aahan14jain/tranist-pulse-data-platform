-- Transit Pulse warehouse schema (Amazon Redshift).
-- Personal portfolio project; not affiliated with any transit agency.
--
-- route_delay_metrics is the analytics-facing table for per-route delay
-- windows produced by the streaming delay calculator and/or historical
-- backfill (curated-zone/route-delay-windows Parquet).

CREATE TABLE IF NOT EXISTS route_delay_metrics (
    route_id           VARCHAR(64)       NOT NULL,
    window_start       TIMESTAMP         NOT NULL,
    window_end         TIMESTAMP         NOT NULL,
    avg_delay_seconds  DOUBLE PRECISION,
    vehicle_count      BIGINT,
    computed_at        TIMESTAMP         NOT NULL DEFAULT GETDATE()
)
-- DISTKEY(route_id): reliability dashboards and route-level SLA queries
-- almost always filter or GROUP BY route_id. Co-locating a route's rows on
-- the same node avoids redistributing large scans for those predicates.
DISTKEY (route_id)
-- SORTKEY(window_start): time-range filters ("last 24 hours", "morning
-- peak on date D") prune zone maps / sorted blocks so Redshift skips
-- windows outside the requested interval.
SORTKEY (window_start);

-- ---------------------------------------------------------------------------
-- Production load path (commented template — not executed by this file).
-- Curated Parquet lands in S3 (or MinIO in local dev). In AWS, either:
--   1) COPY Parquet from S3 into route_delay_metrics, or
--   2) Query in place via Redshift Spectrum over an external table.
-- Prefer COPY when the table is the system of record for BI tools.
-- ---------------------------------------------------------------------------
--
-- -- IAM role must allow s3:GetObject on the curated prefix.
-- COPY route_delay_metrics (
--     route_id,
--     window_start,
--     window_end,
--     avg_delay_seconds,
--     vehicle_count
-- )
-- FROM 's3://curated-zone/route-delay-windows/'
-- IAM_ROLE 'arn:aws:iam::<account-id>:role/<redshift-copy-role>'
-- FORMAT AS PARQUET;
--
-- -- Spectrum alternative: external schema over the same prefix, then
-- -- INSERT INTO route_delay_metrics SELECT ... FROM spectrum.route_delay_windows
-- -- WHERE window_date BETWEEN '2026-07-01' AND '2026-07-03';
--
-- -- Idempotent reload for a date range (pair with load_curated.py logic):
-- -- DELETE FROM route_delay_metrics
-- -- WHERE TRUNC(window_start) BETWEEN '2026-07-01' AND '2026-07-03';
-- -- then COPY only the matching window_date partitions.
