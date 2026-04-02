terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

provider "azurerm" {
  features {
    resource_group {
      # Set to true for safe defaults - prevents accidental deletion of resource groups with resources
      # Set to false ONLY for ephemeral/test environments where automated cleanup is required
      prevent_deletion_if_contains_resources = true
    }
  }

  # Skip provider registration - used when service principal lacks registration permissions
  # or when providers are pre-registered in the subscription. Remove this if you encounter
  # "provider not registered" errors and have sufficient permissions.
  skip_provider_registration = true
}
