terraform {
  required_version = "~> 1.5"

  required_providers {
    newrelic = {
      source = "newrelic/newrelic"
    }
  }

  cloud {
    organization = "TPF"

    workspaces {
      name = "pipelineframework"
    }
  }
}
