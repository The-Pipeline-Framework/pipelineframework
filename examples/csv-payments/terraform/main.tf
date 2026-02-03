data "newrelic_entity" "orchestrator" {
  name       = var.service_names.orchestrator
  domain     = var.newrelic_entity_domain
  type       = var.newrelic_entity_type
  account_id = var.newrelic_account_id
}

data "newrelic_entity" "input" {
  name       = var.service_names.input
  domain     = var.newrelic_entity_domain
  type       = var.newrelic_entity_type
  account_id = var.newrelic_account_id
}

data "newrelic_entity" "output" {
  name       = var.service_names.output
  domain     = var.newrelic_entity_domain
  type       = var.newrelic_entity_type
  account_id = var.newrelic_account_id
}

data "newrelic_entity" "payments_processing" {
  name       = var.service_names.payments_processing
  domain     = var.newrelic_entity_domain
  type       = var.newrelic_entity_type
  account_id = var.newrelic_account_id
}

data "newrelic_entity" "payment_status" {
  name       = var.service_names.payment_status
  domain     = var.newrelic_entity_domain
  type       = var.newrelic_entity_type
  account_id = var.newrelic_account_id
}

data "newrelic_entity" "persistence" {
  name       = var.service_names.persistence
  domain     = var.newrelic_entity_domain
  type       = var.newrelic_entity_type
  account_id = var.newrelic_account_id
}

locals {
  services = {
    orchestrator = {
      guid = data.newrelic_entity.orchestrator.guid
      name = var.service_names.orchestrator
    }
    input = {
      guid = data.newrelic_entity.input.guid
      name = var.service_names.input
    }
    output = {
      guid = data.newrelic_entity.output.guid
      name = var.service_names.output
    }
    payments_processing = {
      guid = data.newrelic_entity.payments_processing.guid
      name = var.service_names.payments_processing
    }
    payment_status = {
      guid = data.newrelic_entity.payment_status.guid
      name = var.service_names.payment_status
    }
    persistence = {
      guid = data.newrelic_entity.persistence.guid
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
  name        = "Orchestrator downstream RPC availability"
  description = "Share of orchestrator downstream RPC calls without errors."

  events {
    account_id = var.newrelic_account_id

    valid_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.rpc.client.total"
      }
      where = "service.name = '${local.services.orchestrator.name}' AND metricName = 'tpf.slo.rpc.client.total'"
    }

    good_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.rpc.client.good"
      }
      where = "service.name = '${local.services.orchestrator.name}' AND metricName = 'tpf.slo.rpc.client.good'"
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
  name        = "Item latency <= 1000ms (core steps)"
  description = "Share of core-step RPC calls within the configured latency threshold."

  events {
    account_id = var.newrelic_account_id

    valid_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.rpc.server.latency.total"
      }
      where = "metricName = 'tpf.slo.rpc.server.latency.total' AND ${local.core_step_name_filter}"
    }

    good_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.rpc.server.latency.good"
      }
      where = "metricName = 'tpf.slo.rpc.server.latency.good' AND ${local.core_step_name_filter}"
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

resource "newrelic_service_level" "item_avg_latency" {
  guid        = local.services.orchestrator.guid
  name        = "Downstream RPC latency <= 1000ms (orchestrator)"
  description = "Share of orchestrator downstream RPC calls within the configured latency threshold."

  events {
    account_id = var.newrelic_account_id

    valid_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.rpc.client.latency.total"
      }
      where = "service.name = '${local.services.orchestrator.name}' AND metricName = 'tpf.slo.rpc.client.latency.total'"
    }

    good_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.rpc.client.latency.good"
      }
      where = "service.name = '${local.services.orchestrator.name}' AND metricName = 'tpf.slo.rpc.client.latency.good'"
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

resource "newrelic_service_level" "items_per_min" {
  guid        = local.services.orchestrator.guid
  name        = "Item throughput >= 1000/min (boundary)"
  description = "Share of pipeline runs that meet the configured throughput threshold."

  events {
    account_id = var.newrelic_account_id

    valid_events {
      from  = "Metric"
      select {
        function  = "COUNT"
        attribute = "tpf.slo.item.throughput.total"
      }
      where = "service.name = '${local.services.orchestrator.name}' AND metricName = 'tpf.slo.item.throughput.total'"
    }

    good_events {
      from  = "Metric"
      select {
        function  = "COUNT"
        attribute = "tpf.slo.item.throughput.good"
      }
      where = "service.name = '${local.services.orchestrator.name}' AND metricName = 'tpf.slo.item.throughput.good'"
    }
  }

  objective {
    target = 90.0
    time_window {
      rolling {
        count = 7
        unit  = "DAY"
      }
    }
  }
}

resource "newrelic_service_level" "item_success_rate" {
  guid        = local.services.orchestrator.guid
  name        = "Item success ratio"
  description = "Produced to consumed item ratio for the item stream."

  events {
    account_id = var.newrelic_account_id

    valid_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.item.success.total"
      }
      where = "service.name = '${local.services.orchestrator.name}' AND metricName = 'tpf.slo.item.success.total'"
    }

    good_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.item.success.good"
      }
      where = "service.name = '${local.services.orchestrator.name}' AND metricName = 'tpf.slo.item.success.good'"
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

resource "newrelic_service_level" "step_reliability" {
  for_each    = local.reliability_services
  guid        = each.value.guid
  name        = "RPC reliability"
  description = "Share of RPC calls without errors for ${each.value.name}."

  events {
    account_id = var.newrelic_account_id

    valid_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.rpc.server.total"
      }
      where = "service.name = '${each.value.name}' AND metricName = 'tpf.slo.rpc.server.total'"
    }

    good_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.rpc.server.good"
      }
      where = "service.name = '${each.value.name}' AND metricName = 'tpf.slo.rpc.server.good'"
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
  name        = "RPC latency <= 1000ms"
  description = "Share of RPC calls within the configured latency threshold for ${each.value.name}."

  events {
    account_id = var.newrelic_account_id

    valid_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.rpc.server.latency.total"
      }
      where = "service.name = '${each.value.name}' AND metricName = 'tpf.slo.rpc.server.latency.total'"
    }

    good_events {
      from  = "Metric"
      select {
        function  = "SUM"
        attribute = "tpf.slo.rpc.server.latency.good"
      }
      where = "service.name = '${each.value.name}' AND metricName = 'tpf.slo.rpc.server.latency.good'"
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
