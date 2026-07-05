variable "aws_region" {
  type        = string
  description = "AWS region for all resources."
  default     = "us-west-2"
}

variable "project_name" {
  type        = string
  description = "Name prefix for resources."
  default     = "transit-pulse"
}

variable "environment" {
  type        = string
  description = "Environment tag (dev / portfolio / prod)."
  default     = "portfolio"
}

variable "raw_bucket_name" {
  type        = string
  description = "Globally unique S3 bucket for the raw zone (gtfs-static/, vehicle-positions/)."
  # Must be unique across all AWS accounts — override in terraform.tfvars.
}

variable "curated_bucket_name" {
  type        = string
  description = "Globally unique S3 bucket for curated route-delay-windows/."
}

variable "redshift_base_capacity_rpu" {
  type        = number
  description = <<-EOT
    Redshift Serverless base capacity in RPUs.
    COST: billed per RPU-hour while the workgroup can resume; idle has a
    minimum footprint. Start at 8 (lowest) for portfolio demos; set to 0
    only if you tear the workgroup down when not demoing.
  EOT
  default     = 8
}

variable "glue_crawler_schedule" {
  type        = string
  description = "Glue crawler cron (UTC). Empty string = on-demand only (cheaper)."
  default     = ""
}
