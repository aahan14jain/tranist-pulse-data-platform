terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment for remote state (incurs S3 + optional DynamoDB cost).
  # backend "s3" {
  #   bucket         = "transit-pulse-tfstate"
  #   key            = "aws/terraform.tfstate"
  #   region         = "us-west-2"
  #   dynamodb_table = "transit-pulse-tf-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
