# Design

Model typed application flows, runtime shape, connectors, and design-time boundaries.

- [Data Architecture](/design/data-architecture/index) for types definition and input/output flows
- [Architectural Decisions](/decisions/) records the durable
  ownership rules and distinctions that should survive implementation changes.

## Connectors and boundaries

- [Await Boundaries](/design/await-boundaries) for deferred external completion.
- [Object Ingest And Publish](/design/object-ingest) for object-store and file-system input/output shells.
- [JPA Query Connector](/design/jpa-query-connector/) for captured database reads that feed business decisions.
- [Persistence Plugin](/design/persistence) for write-side business output storage.

