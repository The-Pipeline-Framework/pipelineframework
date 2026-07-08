---
search: false
---

# Azure Functions Troubleshooting

Use this page when the Search Azure Functions verification lane fails.

## Common Failures

| Symptom | Check |
| --- | --- |
| Function host does not start | Azure Functions Core Tools version, Java 21 availability, generated `host.json` location |
| Handler bootstraps but request fails | generated route names, REST naming strategy, function app logs |
| Cloud deployment fails | Azure credentials, Terraform state, resource group permissions, Quarkus Azure Functions packaging |
| Smoke test times out | cold start budget, function timeout, network path, missing app settings |

## Useful Logs

- Azure Functions host logs
- Quarkus application logs
- GitHub Actions job logs for the manual validation workflow
- Terraform plan/apply output

For the original detailed notes, see [Search Azure Functions Reference](/versions/v26.6.2/deploy/search-azure-functions-reference#troubleshooting).
