# Least-privilege roles for pipeline components.
# Attach pipeline_job to EMR Serverless / ECS task / Airflow workers.
# Attach glue_crawler to the Glue crawler only.
# Attach redshift_copy to Redshift Serverless for COPY from curated S3.

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

# ---------------------------------------------------------------------------
# Glue crawler role
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_crawler" {
  name               = "${var.project_name}-glue-crawler"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "glue_crawler" {
  statement {
    sid     = "ListCuratedBucket"
    actions = ["s3:ListBucket"]
    resources = [
      aws_s3_bucket.curated.arn,
    ]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["route-delay-windows", "route-delay-windows/*"]
    }
  }

  statement {
    sid     = "ReadCuratedObjects"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.curated.arn}/route-delay-windows/*",
    ]
  }

  statement {
    sid = "GlueCatalogWrite"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:BatchCreatePartition",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/${aws_glue_catalog_database.curated.name}",
      "arn:aws:glue:${local.region}:${local.account_id}:table/${aws_glue_catalog_database.curated.name}/*",
    ]
  }

  statement {
    sid = "GlueLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_crawler" {
  name   = "${var.project_name}-glue-crawler"
  role   = aws_iam_role.glue_crawler.id
  policy = data.aws_iam_policy_document.glue_crawler.json
}

# ---------------------------------------------------------------------------
# Pipeline job role (ingestion, Spark batch, warehouse loader)
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "pipeline_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type = "Service"
      identifiers = [
        "elasticmapreduce.amazonaws.com",
        "emr-serverless.amazonaws.com",
        "ecs-tasks.amazonaws.com",
      ]
    }
  }
}

resource "aws_iam_role" "pipeline_job" {
  name               = "${var.project_name}-pipeline-job"
  assume_role_policy = data.aws_iam_policy_document.pipeline_assume.json
}

data "aws_iam_policy_document" "pipeline_job" {
  statement {
    sid = "RawBucketList"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [aws_s3_bucket.raw.arn]
  }

  statement {
    sid = "RawBucketReadWrite"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.raw.arn}/gtfs-static/*",
      "${aws_s3_bucket.raw.arn}/vehicle-positions/*",
    ]
  }

  statement {
    sid = "CuratedBucketList"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [aws_s3_bucket.curated.arn]
  }

  statement {
    sid = "CuratedBucketReadWrite"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.curated.arn}/route-delay-windows/*",
    ]
  }

  # Spark checkpoints if moved off /tmp onto S3.
  statement {
    sid = "SparkCheckpoints"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/spark-checkpoints/*",
    ]
  }

  statement {
    sid = "GlueCatalogRead"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = [
      "arn:aws:glue:${local.region}:${local.account_id}:catalog",
      "arn:aws:glue:${local.region}:${local.account_id}:database/${aws_glue_catalog_database.curated.name}",
      "arn:aws:glue:${local.region}:${local.account_id}:table/${aws_glue_catalog_database.curated.name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "pipeline_job" {
  name   = "${var.project_name}-pipeline-job"
  role   = aws_iam_role.pipeline_job.id
  policy = data.aws_iam_policy_document.pipeline_job.json
}

# ---------------------------------------------------------------------------
# Redshift COPY role (S3 curated → route_delay_metrics)
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_copy" {
  name               = "${var.project_name}-redshift-copy"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

data "aws_iam_policy_document" "redshift_copy" {
  statement {
    sid     = "ListCurated"
    actions = ["s3:ListBucket"]
    resources = [
      aws_s3_bucket.curated.arn,
    ]
  }

  statement {
    sid     = "GetCuratedObjects"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.curated.arn}/route-delay-windows/*",
    ]
  }
}

resource "aws_iam_role_policy" "redshift_copy" {
  name   = "${var.project_name}-redshift-copy"
  role   = aws_iam_role.redshift_copy.id
  policy = data.aws_iam_policy_document.redshift_copy.json
}

# Associate this role with the Redshift Serverless namespace after apply:
#   aws redshift-serverless update-namespace \
#     --namespace-name <name> \
#     --iam-roles <redshift_copy.arn>
