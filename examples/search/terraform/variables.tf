variable "resource_group_name" {
  description = "Name of the resource group to create"
  type        = string
  default     = "rg-search-pipeline-functions"
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "westus"

  validation {
    condition     = contains(["westus", "eastus", "westus2", "eastus2", "northeurope", "westeurope", "southeastasia", "eastasia", "australiaeast", "australiasoutheast", "japaneast", "japanwest", "brazilsouth", "centralus", "northcentralus", "southcentralus", "ukwest", "uksouth", "koreacentral", "koreasouth", "francecentral", "germanywestcentral", "switzerlandnorth", "swedencentral", "uaenorth", "southafricanorth"], var.location)
    error_message = "Location must be a valid Azure region. See https://azure.microsoft.com/en-us/explore/global-infrastructure/geographies for available regions."
  }
}

variable "function_app_name" {
  description = "Name of the Function App (must be globally unique). You must provide a unique value."
  type        = string
  nullable    = false
}

variable "storage_account_name" {
  description = "Name of the Storage Account (must be globally unique). You must provide a unique value."
  type        = string
  nullable    = false
}

variable "app_insights_name" {
  description = "Name of the Application Insights resource"
  type        = string
  default     = "appi-search-pipeline"
}

variable "service_plan_sku" {
  description = "SKU for the App Service Plan (Premium for VNET/cold start mitigation)"
  type        = string
  default     = "EP1" # Elastic Premium 1
}

variable "java_version" {
  description = "Java version for the Function runtime"
  type        = string
  default     = "21"
}

variable "function_worker_runtime" {
  description = "Azure Functions worker runtime"
  type        = string
  default     = "java"

  validation {
    condition     = contains(["dotnet", "dotnet-isolated", "node", "python", "java", "powershell", "custom"], var.function_worker_runtime)
    error_message = "Function worker runtime must be one of: dotnet, dotnet-isolated, node, python, java, powershell, custom."
  }
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Environment = "test"
    Project     = "search-pipeline"
    ManagedBy   = "terraform"
  }
}

variable "enable_app_insights" {
  description = "Whether to enable Application Insights"
  type        = bool
  default     = true
}

variable "cleanup_soft_delete" {
  description = "Whether to purge soft-delete-enabled resources on destroy (e.g., storage accounts, Log Analytics workspace). WARNING: Setting to true will permanently purge soft-deleted resources - use with extreme caution in production."
  type        = bool
  default     = false
}

variable "allowed_cors_origins" {
  description = "List of allowed CORS origins for the Function App"
  type        = list(string)
  default     = []
}
