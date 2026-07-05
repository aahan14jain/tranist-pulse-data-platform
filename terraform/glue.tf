# Glue Data Catalog over curated Parquet for Athena / Spectrum-style discovery.
#
# COST: Crawler runs are billed per DPU-hour. Prefer on-demand crawls
# (glue_crawler_schedule = "") for a portfolio account.

resource "aws_glue_catalog_database" "curated" {
  name        = "${replace(var.project_name, "-", "_")}_curated"
  description = "Transit Pulse curated delay metrics (route-delay-windows)."
}

resource "aws_glue_crawler" "curated_route_delay_windows" {
  name          = "${var.project_name}-curated-route-delay-windows"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.curated.name
  description   = "Infers schema from curated-zone/route-delay-windows/ Parquet."

  s3_target {
    path = "s3://${aws_s3_bucket.curated.bucket}/route-delay-windows/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  # Empty schedule = manual start only (cheapest for demos).
  schedule = var.glue_crawler_schedule != "" ? var.glue_crawler_schedule : null
}
