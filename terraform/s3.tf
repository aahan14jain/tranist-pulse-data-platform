# Raw + curated object storage (MinIO buckets in local compose).
# Prefixes mirror the local layout:
#   s3://raw/gtfs-static/...
#   s3://raw/vehicle-positions/YYYY-MM-DD/...
#   s3://curated/route-delay-windows/...
#
# COST: S3 Standard storage + PUT/GET requests. Portfolio-scale data is
# typically cents/month; watch request volume if Spark lists aggressively.

resource "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket_name
}

resource "aws_s3_bucket" "curated" {
  bucket = var.curated_bucket_name
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "curated" {
  bucket                  = aws_s3_bucket.curated.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "curated" {
  bucket = aws_s3_bucket.curated.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated" {
  bucket = aws_s3_bucket.curated.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Placeholder prefixes so the layout is visible in the console before first write.
resource "aws_s3_object" "raw_prefixes" {
  for_each = toset([
    "gtfs-static/",
    "vehicle-positions/",
  ])
  bucket  = aws_s3_bucket.raw.id
  key     = each.value
  content = ""
}

resource "aws_s3_object" "curated_prefix" {
  bucket  = aws_s3_bucket.curated.id
  key     = "route-delay-windows/"
  content = ""
}
