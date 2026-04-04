variable "aws_region" {
  description = "AWS region for the modular Search Lambda lane."
  type        = string
}

variable "name_prefix" {
  description = "Prefix used for all Lambda and IAM resource names."
  type        = string
}

variable "orchestrator_zip_path" {
  description = "Path to the orchestrator Lambda ZIP."
  type        = string
  default     = "../../orchestrator-svc/target/function.zip"
}

variable "crawl_source_zip_path" {
  description = "Path to the crawl-source Lambda ZIP."
  type        = string
  default     = "../../crawl-source-svc/target/function.zip"
}

variable "parse_document_zip_path" {
  description = "Path to the parse-document Lambda ZIP."
  type        = string
  default     = "../../parse-document-svc/target/function.zip"
}

variable "tokenize_content_zip_path" {
  description = "Path to the tokenize-content Lambda ZIP."
  type        = string
  default     = "../../tokenize-content-svc/target/function.zip"
}

variable "index_document_zip_path" {
  description = "Path to the index-document Lambda ZIP."
  type        = string
  default     = "../../index-document-svc/target/function.zip"
}
