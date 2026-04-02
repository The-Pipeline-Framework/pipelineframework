output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.search_pipeline.name
}

output "function_app_name" {
  description = "Name of the Function App"
  value       = try(azurerm_linux_function_app.search_pipeline.name, azurerm_windows_function_app.search_pipeline.name)
}

output "function_app_default_hostname" {
  description = "Default hostname of the Function App"
  value       = try(azurerm_linux_function_app.search_pipeline.default_hostname, azurerm_windows_function_app.search_pipeline.default_hostname)
}

output "function_app_url" {
  description = "Base URL of the Function App"
  value       = "https://${try(azurerm_linux_function_app.search_pipeline.default_hostname, azurerm_windows_function_app.search_pipeline.default_hostname)}"
}

output "function_app_identity_principal_id" {
  description = "Principal ID of the Function App's managed identity"
  value       = try(azurerm_linux_function_app.search_pipeline.identity.0.principal_id, azurerm_windows_function_app.search_pipeline.identity.0.principal_id, null)
}

output "storage_account_name" {
  description = "Name of the Storage Account"
  value       = azurerm_storage_account.function_storage.name
}

output "storage_account_id" {
  description = "Resource ID of the Storage Account"
  value       = azurerm_storage_account.function_storage.id
}

output "application_insights_id" {
  description = "Resource ID of Application Insights"
  value       = var.enable_app_insights ? azurerm_application_insights.search_pipeline.0.id : null
}

output "application_insights_connection_string" {
  description = "Connection string for Application Insights"
  value       = var.enable_app_insights ? azurerm_application_insights.search_pipeline.0.connection_string : null
  sensitive   = true
}

output "log_analytics_workspace_id" {
  description = "Resource ID of Log Analytics Workspace"
  value       = var.enable_app_insights ? azurerm_log_analytics_workspace.search_pipeline.0.id : null
}

output "service_plan_id" {
  description = "Resource ID of the App Service Plan"
  value       = azurerm_service_plan.function_plan.id
}

output "location" {
  description = "Azure region where resources were created"
  value       = azurerm_resource_group.search_pipeline.location
}
