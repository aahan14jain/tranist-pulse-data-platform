output "raw_bucket" {
  value       = aws_s3_bucket.raw.bucket
  description = "Set RAW_ZONE_BUCKET to this value."
}

output "curated_bucket" {
  value       = aws_s3_bucket.curated.bucket
  description = "Set CURATED_ZONE_BUCKET to this value."
}

output "glue_database" {
  value = aws_glue_catalog_database.curated.name
}

output "glue_crawler_name" {
  value = aws_glue_crawler.curated_route_delay_windows.name
}

output "redshift_workgroup" {
  value = aws_redshiftserverless_workgroup.main.workgroup_name
}

output "redshift_namespace" {
  value = aws_redshiftserverless_namespace.main.namespace_name
}

output "pipeline_job_role_arn" {
  value       = aws_iam_role.pipeline_job.arn
  description = "Attach to EMR Serverless / batch job execution role."
}

output "redshift_copy_role_arn" {
  value       = aws_iam_role.redshift_copy.arn
  description = "Set REDSHIFT_COPY_IAM_ROLE; associate with the Redshift namespace."
}

output "env_snippet" {
  description = "Example env vars for AWS (no secrets — use IAM roles)."
  value       = <<-EOT
    AWS_REGION=${var.aws_region}
    RAW_ZONE_BUCKET=${aws_s3_bucket.raw.bucket}
    CURATED_ZONE_BUCKET=${aws_s3_bucket.curated.bucket}
    # Leave MINIO_ENDPOINT unset so boto3/Spark use real S3 + IAM.
    # MINIO_ACCESS_KEY / MINIO_SECRET_KEY unset (use instance/task role).
    WAREHOUSE_BACKEND=redshift
    WAREHOUSE_HOST=<redshift-workgroup-endpoint>
    WAREHOUSE_PORT=5439
    WAREHOUSE_DATABASE=transit_pulse
    REDSHIFT_COPY_IAM_ROLE=${aws_iam_role.redshift_copy.arn}
    KAFKA_BOOTSTRAP_SERVERS=<msk-bootstrap-or-keep-local>
  EOT
}
