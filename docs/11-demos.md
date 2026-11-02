# Step 11: Demo Scenarios

> [↑ Setup overview](../openshift-setup.md) · prev: [Step 10: Custom filter](10-custom-filter.md) · next: [Step 12: Demo walkthrough](12-demos-walkthrough.md)

Ready-made walkthroughs for demonstrating the stack — one document per demo under [`docs/demos/`](demos). For running them as a **single presentation**, see the [end-to-end demo walkthrough](12-demos-walkthrough.md) — the demos in a building order with talking points, timing, and the revert between acts.

Scenarios 1, 3–5 and 12–14 need **no new infrastructure** — they exercise what is already deployed (see the [steps 6–20](../openshift-setup.md)). Scenarios 6–8 and 11 apply a demo manifest from [`deploy/demos/`](../deploy/README.md) that **replaces** the `kafka-cluster-1` `VirtualKafkaCluster` wiring — revert each with `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`. Scenario 2 applies a manifest that **adds** a demo topic and a Record Validation filter (revert by removing the `filterRefs` entry).

All commands run from the **repository root**.

| # | Scenario | Needs |
|---|---|---|
| 1 | [Token revocation (OAuth payoff)](demos/01-token-revocation.md) | — |
| 2 | [Record Validation end-to-end](demos/02-record-validation.md) | `deploy/demos/02-record-validation-demo.yaml` |
| 3 | [Live migration / cutover (MM2)](demos/03-live-migration.md) | — |
| 4 | [DMZ pivot attempt](demos/04-dmz-pivot.md) | — |
| 5 | [Stable endpoints](demos/05-stable-endpoints.md) | — |
| 6 | [Authorization at the proxy](demos/06-proxy-authorization.md) | `deploy/demos/06-authorization-filters.yaml` |
| 7 | [Entity isolation (preview)](demos/07-entity-isolation.md) | `deploy/demos/07-entity-isolation.yaml` |
| 8 | [Token validation on the Kafka cluster](demos/08-token-validation.md) | broker-side JWKS validation (proxy-layer variant is upstream-build only) |
| 9 | [Multi-tenant virtual clusters](demos/09-multi-tenant-virtual-clusters.md) | `deploy/demos/09-multi-tenant-virtual-clusters.yaml` (optional) |
| 10 | [MM2 failover / failback](demos/10-mm2-failover.md) | `deploy/demos/10-mm2-failover.yaml` |
| 11 | [Record Encryption at rest](demos/11-record-encryption.md) | `deploy/demos/11-record-encryption.yaml` (Vault dev) |
| 12 | [Schema evolution](demos/12-schema-evolution.md) | registry from Step 5.4 |
| 13 | [Custom filter](demos/13-custom-filter.md) | docs [Step 10](10-custom-filter.md) |
| 14 | [Disaster recovery active/passive](demos/14-disaster-recovery.md) | — |

## The combined narrative

> (5) stable endpoints → (1) OAuth-secured data path → (1) revoke a client live → (2/12) validation/contract enforcement at the proxy → (3/14) mirror + cutover with clients unaware → (6/7/8) authorization, isolation, and token checks at the edge → (11) encryption at rest the brokers can't read → all of it visible in (15) the dashboards ([Step 1](01-user-workload-monitoring.md)).
