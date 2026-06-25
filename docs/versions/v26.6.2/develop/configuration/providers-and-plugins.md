---
title: Providers and Plugins
search: false
---

# Providers and Plugins

Provider and plugin configuration selects framework-owned shell behavior around the pure application core.

## Main Surfaces

| Surface | Purpose | Full reference |
| --- | --- | --- |
| Persistence provider | durable business records | [Persistence Configuration](/versions/v26.6.2/develop/configuration/all-settings#persistence-configuration) |
| Cache provider and policy | reusable deterministic outputs | [Cache Configuration](/versions/v26.6.2/develop/configuration/all-settings#cache-configuration) |
| Repository materialization | payload references and claim-check storage | [Repository Materialization Configuration](/versions/v26.6.2/develop/configuration/all-settings#repository-materialization-configuration) |
| Item reject sink | terminal rejected item handling | [Item Reject Sink](/versions/v26.6.2/develop/configuration/all-settings#item-reject-sink) |

## Related Design Guides

- [Persistence](/versions/v26.6.2/design/persistence)
- [Caching](/versions/v26.6.2/design/caching/)
- [Field Materialization](/versions/v26.6.2/design/materialization)
- [State Model](/versions/v26.6.2/design/state-model)
- [Using Plugins](/versions/v26.6.2/develop/using-plugins)

