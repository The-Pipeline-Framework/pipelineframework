---
title: Providers and Plugins
---

# Providers and Plugins

Provider and plugin configuration selects framework-owned shell behavior around the pure application core.

## Main Surfaces

| Surface | Purpose | Full reference |
| --- | --- | --- |
| Persistence provider | durable business records | [Persistence Configuration](/develop/configuration/all-settings#persistence-configuration) |
| Cache provider and policy | reusable deterministic outputs | [Cache Configuration](/develop/configuration/all-settings#cache-configuration) |
| Repository materialization | payload references and claim-check storage | [Repository Materialization Configuration](/develop/configuration/all-settings#repository-materialization-configuration) |
| Item reject sink | terminal rejected item handling | [Item Reject Sink](/develop/configuration/all-settings#item-reject-sink) |

## Related Design Guides

- [Persistence](/design/persistence)
- [Caching](/design/caching/)
- [Field Materialization](/design/materialization)
- [State Model](/design/state-model)
- [Using Plugins](/develop/using-plugins)
