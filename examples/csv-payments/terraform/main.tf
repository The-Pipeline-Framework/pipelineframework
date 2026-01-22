data "newrelic_entity" "services" {
  for_each = var.service_names
  name     = each.value
  domain   = var.newrelic_entity_domain
  type     = var.newrelic_entity_type
  account_id = var.newrelic_account_id
}

locals {
  services = {
    orchestrator = {
      guid = data.newrelic_entity.services["orchestrator"].guid
      name = var.service_names.orchestrator
    }
    input = {
      guid = data.newrelic_entity.services["input"].guid
      name = var.service_names.input
    }
    output = {
      guid = data.newrelic_entity.services["output"].guid
      name = var.service_names.output
    }
    payments_processing = {
      guid = data.newrelic_entity.services["payments_processing"].guid
      name = var.service_names.payments_processing
    }
    payment_status = {
      guid = data.newrelic_entity.services["payment_status"].guid
      name = var.service_names.payment_status
    }
    persistence = {
      guid = data.newrelic_entity.services["persistence"].guid
      name = var.service_names.persistence
    }
  }

  core_step_services = {
    payments_processing = local.services.payments_processing
    payment_status      = local.services.payment_status
    persistence         = local.services.persistence
  }

  reliability_services = {
    input               = local.services.input
    output              = local.services.output
    payments_processing = local.services.payments_processing
    payment_status      = local.services.payment_status
    persistence         = local.services.persistence
  }

  core_step_names       = [for svc in values(local.core_step_services) : svc.name]
  core_step_name_filter = "service.name IN ('${join("' , '", local.core_step_names)}')"
}

resource "newrelic_service_level" "orchestrator_availability" {
  guid        = local.services.orchestrator.guid
  name        = "Orchestrator availability"
  description = "Pipeline runs that complete without errors for orchestrator-svc."

  events {
    account_id = var.newrelic_account_id

    valid_events {
      from  = "Span"
      where = "service.name = '${local.services.orchestrator.name}' AND name = 'tpf.pipeline.run'"
    }

    good_events {
      from  = "Span"
      where = "service.name = '${local.services.orchestrator.name}' AND name = 'tpf.pipeline.run' AND (status.code IS NULL OR status.code != 'ERROR')"
    }
  }

  objective {
    target = 99.0
    time_window {
      rolling {
        count = 7
        unit  = "DAY"
      }
    }
  }
}

resource "newrelic_service_level" "row_latency" {
  guid        = local.services.orchestrator.guid
  name        = "Row latency (core steps)"
  description = "Approx per-row latency from core step gRPC spans (per-item tracing not required)."

  events {
    account_id = var.newrelic_account_id

    valid_events {
      from  = "Span"
      where = "span.kind = 'server' AND rpc.system = 'grpc' AND ${local.core_step_name_filter}"
    }

    good_events {
      from  = "Span"
      where = "span.kind = 'server' AND rpc.system = 'grpc' AND ${local.core_step_name_filter} AND duration < 0.25"
    }
  }

  objective {
    target = 95.0
    time_window {
      rolling {
        count = 7
        unit  = "DAY"
      }
    }
  }
}

resource "newrelic_service_level" "step_reliability" {
  for_each    = local.reliability_services
  guid        = each.value.guid
  name        = "RPC reliability"
  description = "Share of gRPC requests without span errors for ${each.value.name}."

  events {
    account_id = var.newrelic_account_id

    valid_events {
      from  = "Span"
      where = "service.name = '${each.value.name}' AND span.kind = 'server' AND rpc.system = 'grpc'"
    }

    good_events {
      from  = "Span"
      where = "service.name = '${each.value.name}' AND span.kind = 'server' AND rpc.system = 'grpc' AND (status.code IS NULL OR status.code != 'ERROR')"
    }
  }

  objective {
    target = 99.0
    time_window {
      rolling {
        count = 7
        unit  = "DAY"
      }
    }
  }
}

resource "newrelic_service_level" "step_latency" {
  for_each    = local.core_step_services
  guid        = each.value.guid
  name        = "RPC latency"
  description = "Share of gRPC requests under 500ms for ${each.value.name}."

  events {
    account_id = var.newrelic_account_id

    valid_events {
      from  = "Span"
      where = "service.name = '${each.value.name}' AND span.kind = 'server' AND rpc.system = 'grpc'"
    }

    good_events {
      from  = "Span"
      where = "service.name = '${each.value.name}' AND span.kind = 'server' AND rpc.system = 'grpc' AND duration < 0.5"
    }
  }

  objective {
    target = 95.0
    time_window {
      rolling {
        count = 7
        unit  = "DAY"
      }
    }
  }
}
