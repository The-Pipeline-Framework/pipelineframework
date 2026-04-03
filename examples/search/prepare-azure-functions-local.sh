#!/usr/bin/env bash
set -euo pipefail

# Script to prepare local Azure Functions testing environment
# Creates required host.json and local.settings.json files for 'func host start'
#
# Note: This script uses an HTTP trigger wrapper to bootstrap Quarkus for local testing.
# The TPF framework generates native Azure Functions handlers for pipeline steps,
# but local testing requires this HTTP trigger wrapper to route requests to Quarkus REST endpoints.
# For production deployment, use 'quarkus:deploy' which generates the complete function structure.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORCHESTRATOR_DIR="$SCRIPT_DIR/orchestrator-svc"
TARGET_DIR="$ORCHESTRATOR_DIR/target"
HTTP_PORT=7071

# Verify orchestrator-svc directory exists
if [[ ! -d "$ORCHESTRATOR_DIR" ]]; then
    echo "ERROR: Orchestrator directory not found: $ORCHESTRATOR_DIR" >&2
    echo "Make sure you are running this script from the examples/search directory." >&2
    exit 1
fi

# Verify Azure Functions Core Tools is installed
if ! command -v func &> /dev/null; then
    echo "ERROR: Azure Functions Core Tools (func) is not installed." >&2
    echo "Install it with:" >&2
    echo "  macOS:   brew install azure-functions-core-tools@4" >&2
    echo "  Windows: choco install azure-functions-core-tools-4" >&2
    echo "  Linux:   See https://learn.microsoft.com/azure/azure-functions/functions-run-local" >&2
    exit 1
fi

# Check Core Tools version
FUNC_VERSION=$(func --version 2>&1 || echo "unknown")
if [[ ! "$FUNC_VERSION" =~ ^4\. ]]; then
    echo "WARNING: Azure Functions Core Tools version $FUNC_VERSION detected." >&2
    echo "         Version 4.x is recommended. Some features may not work correctly." >&2
fi

echo "Preparing Azure Functions local testing environment..."
echo ""
echo "Note: This script uses an HTTP trigger wrapper to bootstrap Quarkus for local testing."
echo "      TPF generates native Azure Functions handlers, but local testing requires"
echo "      this HTTP trigger wrapper to route requests to Quarkus REST endpoints."
echo ""

# Create host.json for Azure Functions Core Tools
cat > "$SCRIPT_DIR/host.json" << 'EOF'
{
  "version": "2.0",
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle",
    "version": "[4.*, 5.0.0)"
  },
  "functionTimeout": "00:10:00",
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": false
      }
    },
    "logLevel": {
      "default": "Information",
      "Host.Results": "Information",
      "Host.Aggregator": "Information",
      "Host.Executor": "Information"
    }
  },
  "extensions": {
    "http": {
      "routePrefix": ""
    }
  }
}
EOF

# Create local.settings.json for local development
cat > "$SCRIPT_DIR/local.settings.json" << EOF
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "java",
    "FUNCTIONS_EXTENSION_VERSION": "~4",
    "QUARKUS_PROFILE": "azure-functions",
    "QUARKUS_AZURE_FUNCTIONS_APP_NAME": "search-pipeline-local",
    "QUARKUS_HTTP_PORT": "$HTTP_PORT"
  }
}
EOF

# Create function directory structure in orchestrator-svc/target
# This is a catch-all HTTP trigger that routes to Quarkus
FUNCTION_DIR="$TARGET_DIR/azure-functions/HttpQuarkus"
mkdir -p "$FUNCTION_DIR"

# Create function.json for the HTTP trigger - routes all requests to Quarkus
# SECURITY NOTE: authLevel is set to "anonymous" for LOCAL DEVELOPMENT ONLY.
# For production deployments, change to "function" or "admin" and configure
# appropriate authentication/authorization mechanisms.
cat > "$FUNCTION_DIR/function.json" << EOF
{
  "bindings": [
    {
      "authLevel": "anonymous",
      "type": "httpTrigger",
      "direction": "in",
      "name": "request",
      "methods": ["get", "post", "put", "delete", "patch", "options"],
      "route": "{*route}"
    },
    {
      "type": "http",
      "direction": "out",
      "name": "response"
    }
  ]
}
EOF

# Create local.settings.development.json for local development with storage emulator
# This file is an alternative to local.settings.json when using Azurite
cat > "$SCRIPT_DIR/local.settings.development.json" << EOF
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "java",
    "FUNCTIONS_EXTENSION_VERSION": "~4",
    "QUARKUS_PROFILE": "azure-functions",
    "QUARKUS_HTTP_PORT": "$HTTP_PORT"
  }
}
EOF

echo "✓ Created host.json (with empty routePrefix for direct Quarkus routing)"
echo "✓ Created local.settings.json"
echo "✓ Created HTTP trigger function at $FUNCTION_DIR"
echo ""
echo "You can now run: func host start --java"
echo ""
echo "Available endpoints (after Quarkus starts):"
echo "  - Health:     http://localhost:$HTTP_PORT/q/health"
echo "  - OpenAPI:    http://localhost:$HTTP_PORT/q/openapi"
echo "  - Pipeline:   http://localhost:$HTTP_PORT/api/pipeline/run (example)"
echo ""
echo "Note: The Quarkus Azure Functions extension will bootstrap Quarkus"
echo "      and handle HTTP routing through its REST resources."
echo ""
echo "Prerequisites:"
echo "  - Azure Functions Core Tools v4.x must be installed (verified above)"
echo "  - AzureWebJobsStorage is set to UseDevelopmentStorage=true"
echo "  - You need a local storage emulator running (Azurite or Azure Storage Emulator)"
echo "  - To start Azurite: azurite --silent --location /tmp/azurite"
echo ""
