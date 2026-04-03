resource "azurerm_resource_group" "search_pipeline" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

# Storage Account for Function runtime
resource "azurerm_storage_account" "function_storage" {
  name                     = var.storage_account_name
  location                 = azurerm_resource_group.search_pipeline.location
  resource_group_name      = azurerm_resource_group.search_pipeline.name
  account_tier             = "Standard"
  account_replication_type = "LRS"
  enable_https_traffic_only = true
  min_tls_version          = "TLS1_2"
  allow_shared_key_access  = false

  # Network security - restrict access to function app only
  network_rules {
    default_action = "Deny"
    bypass         = ["AzureServices"]
  }

  tags = var.tags
}

# App Service Plan (Premium SKU for better cold start performance)
resource "azurerm_service_plan" "function_plan" {
  name                = "plan-${var.function_app_name}"
  location            = azurerm_resource_group.search_pipeline.location
  resource_group_name = azurerm_resource_group.search_pipeline.name
  os_type             = "Linux"
  sku_name            = var.service_plan_sku
  worker_count        = 1

  tags = var.tags
}

# Function App (Linux)
resource "azurerm_linux_function_app" "search_pipeline" {
  name                        = var.function_app_name
  location                    = azurerm_resource_group.search_pipeline.location
  resource_group_name         = azurerm_resource_group.search_pipeline.name
  storage_account_name        = azurerm_storage_account.function_storage.name
  storage_uses_managed_identity = true
  service_plan_id             = azurerm_service_plan.function_plan.id
  https_only                  = true
  public_network_access_enabled = true

  site_config {
    application_stack {
      java_version = var.java_version
    }

    # Premium plan settings for better performance
    always_on                      = true
    minimum_tls_version            = "1.2"
    ftps_state                     = "Disabled"
    health_check_path              = "/q/health/live"
    health_check_eviction_time_in_min = 10

    cors {
      allowed_origins     = var.allowed_cors_origins
      support_credentials = false
    }
  }

  app_settings = {
    "FUNCTIONS_WORKER_RUNTIME"              = var.function_worker_runtime
    "FUNCTIONS_EXTENSION_VERSION"           = "~4"
    "JAVA_HOME"                             = "/usr/lib/jvm/java-21-openjdk-amd64"
    "QUARKUS_AZURE_FUNCTIONS_APP_NAME"      = var.function_app_name
    "QUARKUS_AZURE_FUNCTIONS_REGION"        = var.location
    
    # Application Insights configuration
    "APPLICATIONINSIGHTS_CONNECTION_STRING" = var.enable_app_insights ? azurerm_application_insights.search_pipeline.0.connection_string : ""
    "APPINSIGHTS_INSTRUMENTATIONKEY"        = var.enable_app_insights ? azurerm_application_insights.search_pipeline.0.instrumentation_key : ""
    
    # Quarkus-specific settings
    "QUARKUS_PROFILE"                       = "azure-functions"
    "QUARKUS_LOG_LEVEL"                     = "INFO"
    "QUARKUS_LOG_CONSOLE_ENABLE"            = "true"
    
    # Pipeline configuration
    "TPF_BUILD_PLATFORM"                    = "FUNCTION"
    "TPF_BUILD_TRANSPORT"                   = "REST"
    "TPF_BUILD_REST_NAMING_STRATEGY"        = "RESOURCEFUL"
  }

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags

  lifecycle {
    ignore_changes = [
      # Ignore changes to app_settings that may be made by deployment tools
      app_settings["WEBSITE_RUN_FROM_PACKAGE"],
    ]
  }
}

# Log Analytics Workspace (required for App Insights)
# Defined before Application Insights to make dependency explicit
resource "azurerm_log_analytics_workspace" "search_pipeline" {
  count = var.enable_app_insights ? 1 : 0

  name                = "log-${var.function_app_name}"
  location            = azurerm_resource_group.search_pipeline.location
  resource_group_name = azurerm_resource_group.search_pipeline.name
  sku                 = "PerGB2018"
  retention_in_days   = 30

  tags = var.tags
}

# Application Insights (optional)
resource "azurerm_application_insights" "search_pipeline" {
  count = var.enable_app_insights ? 1 : 0

  name                = var.app_insights_name
  location            = azurerm_resource_group.search_pipeline.location
  resource_group_name = azurerm_resource_group.search_pipeline.name
  application_type    = "web"
  workspace_id        = azurerm_log_analytics_workspace.search_pipeline.0.id
  retention_in_days   = 30

  tags = var.tags
}

# Role assignment for Function App to access Storage
resource "azurerm_role_assignment" "function_storage_blob_contributor" {
  scope                = azurerm_storage_account.function_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_function_app.search_pipeline.identity.0.principal_id
}
