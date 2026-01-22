variable "newrelic_account_id" {
  type        = number
  description = "New Relic account ID."
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
