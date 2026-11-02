# Demo 13: Custom filter

> [↑ Demo scenarios](../11-demos.md) · prev: [Demo 12: Schema evolution](12-schema-evolution.md) · next: [Demo 14: Disaster recovery active/passive](14-disaster-recovery.md)

Write, build, and deploy a custom filter in the data path — the full extension story ([Step 10](../10-custom-filter.md)):

1. Generate a filter project with the upstream Maven archetype, implement a small transformation (e.g. inject a `demo-filter` header, or mask a field).
2. Build in-cluster (`deploy/custom-filter/01-buildconfig.yaml`), wire a `KafkaProtocolFilter` ([`deploy/custom-filter/02-kafkaprotocolfilter.yaml`](../../deploy/custom-filter/02-kafkaprotocolfilter.yaml)) into the virtual cluster's `filterRefs`.
3. Show the effect through the proxy; show the filter's absence when producing directly to the broker.
