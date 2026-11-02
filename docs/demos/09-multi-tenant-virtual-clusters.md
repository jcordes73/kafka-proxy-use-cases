# Demo 9: Multi-tenant virtual clusters

> [↑ Demo scenarios](../11-demos.md) · prev: [Demo 8: Token validation at the proxy](08-token-validation.md) · next: [Demo 10: MM2 failover / failback](10-mm2-failover.md)

One proxy, many logical clusters — each with its own bootstrap, filters, and policies:

- The stack already exposes **two** virtual clusters over two physical clusters ([deploy/proxy/05](../../deploy/proxy/05-virtualkafkacluster.yaml)): `kafka-cluster-1` and `kafka-cluster-2`. Point two clients at the two bootstraps — same proxy, two logical clusters.
- [`deploy/demos/09-multi-tenant-virtual-clusters.yaml`](../../deploy/demos/09-multi-tenant-virtual-clusters.yaml) adds a **third** virtual cluster over the *same* physical backend as cluster-1 (`shared-cluster-1`) — showing logical clusters are decoupled from physical ones. Each carries its own `filterRefs`, so per-tenant policies (Demo 6 rules) differ per virtual cluster.
- Revert: `oc delete virtualkafkacluster/shared-cluster-1 -n kafka-proxy`
