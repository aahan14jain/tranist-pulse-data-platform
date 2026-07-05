# Redshift Serverless — analytics warehouse for route_delay_metrics.
# Apply warehouse/redshift_schema.sql after the workgroup is available.
#
# COST: Redshift Serverless bills for RPU-hours when queries run and keeps a
# small base capacity. This is usually the most expensive piece of a portfolio
# deploy. Tear down (terraform destroy) when not demoing, or leave documented
# but undeployed and keep using local Postgres via warehouse/load_curated.py.

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name = "${var.project_name}-${var.environment}"
  db_name       = "transit_pulse"
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "${var.project_name}-${var.environment}"

  base_capacity = var.redshift_base_capacity_rpu
  publicly_accessible = false

  # Attach to default VPC for a minimal skeleton. Production should use a
  # dedicated VPC + private subnets + security groups (not free-tier sensitive,
  # but required for least-privilege networking).
  # subnet_ids         = [...]
  # security_group_ids = [...]
}
