"""One-off: inspect curated route-delay-windows Parquet in MinIO."""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Allow `python scripts/read_curated_delays.py` without installing the package.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv

from streaming.delay_calculator_job import build_spark_session

CURATED_PATH = "s3a://curated-zone/route-delay-windows/"


def main() -> None:
    load_dotenv(_PROJECT_ROOT / ".env")
    minio_endpoint = os.environ["MINIO_ENDPOINT"]
    minio_access_key = os.environ["MINIO_ACCESS_KEY"]
    minio_secret_key = os.environ["MINIO_SECRET_KEY"]

    spark = build_spark_session(minio_endpoint, minio_access_key, minio_secret_key)
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(CURATED_PATH)
        df.orderBy(df.window_start.desc()).show(20, truncate=False)
        print(f"Total rows: {df.count()}")
        print(f"Distinct routes: {df.select('route_id').distinct().count()}")
        df.describe("avg_delay_seconds").show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
