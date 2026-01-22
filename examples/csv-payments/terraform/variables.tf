variable "newrelic_account_id" {
  type        = number
  description = "New Relic account ID."
}

variable "newrelic_api_key" {
  type        = string
  sensitive   = true
  description = "New Relic user API key."
}

variable "newrelic_entity_domain" {
  type        = string
  default     = "EXT"
  description = "New Relic entity domain for services."
}

variable "newrelic_entity_type" {
  type        = string
  default     = "SERVICE"
  description = "New Relic entity type for services."
}

variable "service_names" {
  type = map(string)
  default = {
    orchestrator        = "orchestrator-svc"
    input               = "input-csv-file-processing-svc"
    output              = "output-csv-file-processing-svc"
    payments_processing = "payments-processing-svc"
    payment_status      = "payment-status-svc"
    persistence         = "persistence-svc"
  }
  description = "Service names keyed by role for GUID resolution."
}
