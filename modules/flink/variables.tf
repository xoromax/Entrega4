variable "environment" {
  description = "The environment for the Flink cluster"
  type        = string
}

variable "aws_region" {
  description = "The AWS region for the Flink cluster"
  type        = string
  default     = "us-east-1"
}

variable "aws_account_id" {
  description = "The AWS account ID for the Flink cluster"
  type        = string
}

variable "kinesis_stream_name" {
  description = "The name of the Kinesis stream for the Flink cluster"
  type        = string
}

variable "kinesis_stream_arn" {
  description = "The ARN of the Kinesis stream for the Flink cluster"
  type        = string
}

variable "kms_key_arn" {
  description = "The ARN of the KMS key for the Flink cluster"
  type        = string
}

variable "artifact_bucket_name" {
  description = "The name of the S3 bucket for Flink artifacts"
  type        = string
}

variable "artifact_bucket_arn" {
  description = "The ARN of the S3 bucket for Flink artifacts"
  type        = string
}

variable "flink_jar_path" {
  description = "The path to the Flink JAR file"
  type        = string
}

variable "flink_jar_key" {
  description = "The key of the Flink JAR file in the S3 bucket"
  type        = string
  default     = "flink/urban-sensors-flink.jar"
}

variable "application_name" {
  description = "The name of the application"
  type        = string
  default     = "urban-sensors-flink"
}

variable "runtime_environment" {
  description = "The runtime environment for the Flink cluster"
  type        = string
  default     = "FLINK-1_19"
}

variable "checkpoint_interval_ms" {
  description = "The interval for checkpointing in milliseconds"
  type        = number
  default     = 60000

  validation {
    condition     = var.checkpoint_interval_ms >= 1000
    error_message = "Checkpointing interval must be at least 1000 milliseconds"
  }
}

variable "checkpoint_min_pause_ms" {
  description = "The minimum pause between checkpoints in milliseconds"
  type        = number
  default     = 30000

  validation {
    condition     = var.checkpoint_min_pause_ms >= 0
    error_message = "Minimum pause between checkpoints must be a positive number"
  }
}

variable "parallelism" {
  description = "The parallelism for the Flink job"
  type        = number
  default     = 1

  validation {
    condition     = var.parallelism >= 1
    error_message = "Parallelism must be a positive number"
  }
}

variable "parallelism_per_kpu" {
  description = "The parallelism per KPU for the Flink job"
  type        = number
  default     = 1

  validation {
    condition     = var.parallelism_per_kpu >= 1
    error_message = "Parallelism per KPU must be a positive number"
  }
}

variable "auto_scaling_enabled" {
  description = "Whether autoscaling is enabled for the Flink cluster"
  type        = bool
  default     = false
}

variable "log_level" {
  description = "The log level for the Flink cluster"
  type        = string
  default     = "INFO"

  validation {
    condition = contains(
      ["DEBUG", "INFO", "WARN", "ERROR"],
      var.log_level
    )

    error_message = "Log level must be one of: DEBUG, INFO, WARN, ERROR"
  }
}

variable "metrics_level" {
  description = "The metrics level for the Flink cluster"
  type        = string
  default     = "TASK"

  validation {
    condition = contains(
      ["APPLICATION", "TASK", "OPERATOR", "PARALLELISM"],
      var.metrics_level
    )

    error_message = "Metrics level must be one of: APPLICATION, TASK, OPERATOR, PARALLELISM"
  }
}
