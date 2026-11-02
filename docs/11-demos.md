# Step 11: Demo Scenarios

> [↑ Setup overview](../openshift-setup.md) · next: [Step 12: Demo walkthrough](12-demos-walkthrough.md)

Ready-made walkthroughs for demonstrating the stack — one document per demo under [`docs/demos/`](demos). For running them as a **single presentation**, see the [end-to-end demo walkthrough](12-demos-walkthrough.md) — the demos in a building order with talking points, timing, and the revert between acts. The table below is ordered like the [Use Case Catalog](../README.md#use-case-catalog); the document file names keep the original demo numbers.

Scenarios 1, 2, 6–8, 10 and 12 need **no new infrastructure** — they exercise what is already deployed (see the [setup steps](../openshift-setup.md#guide-map)). Scenarios 3, 4 and 13 apply a demo manifest from [`deploy/demos/`](../deploy/README.md) that **replaces** the `kafka-cluster-1` `VirtualKafkaCluster` wiring — revert each with `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`. Scenario 5 applies a manifest that **adds** a third virtual cluster (revert with `oc delete virtualkafkacluster/shared-cluster-1 -n kafka-proxy`), and scenario 9 replaces the running `KafkaMirrorMaker2` (same name). Scenario 11 applies a manifest that **adds** a demo topic and a Record Validation filter (revert by removing the `filterRefs` entry).

All commands run from the **repository root**.

| # | Use case | Scenario | Needs |
|---|---|---|---|
| 1 | [§2 Stable Client Endpoints & TLS Termination](../README.md#2-stable-client-endpoints--tls-termination) | [Stable endpoints](demos/05-stable-endpoints.md) | — |
| 2 | [§2 Stable Client Endpoints & TLS Termination](../README.md#2-stable-client-endpoints--tls-termination) | [DMZ pivot attempt](demos/04-dmz-pivot.md) | — |
| 3 | [§3 Multi-Tenancy on a Shared Cluster](../README.md#3-multi-tenancy-on-a-shared-cluster) | [Authorization at the proxy](demos/06-proxy-authorization.md) | `deploy/demos/06-authorization-filters.yaml` |
| 4 | [§3 Multi-Tenancy on a Shared Cluster](../README.md#3-multi-tenancy-on-a-shared-cluster) | [Entity isolation (preview)](demos/07-entity-isolation.md) | `deploy/demos/07-entity-isolation.yaml` |
| 5 | [§3 Multi-Tenancy on a Shared Cluster](../README.md#3-multi-tenancy-on-a-shared-cluster) | [Multi-tenant virtual clusters](demos/09-multi-tenant-virtual-clusters.md) | `deploy/demos/09-multi-tenant-virtual-clusters.yaml` (optional) |
| 6 | [§4 OIDC / OAuth 2.0 Authentication](../README.md#4-oidc--oauth-20-authentication-with-red-hat-build-of-keycloak) | [Token revocation (OAuth payoff)](demos/01-token-revocation.md) | — |
| 7 | [§4 OIDC / OAuth 2.0 Authentication](../README.md#4-oidc--oauth-20-authentication-with-red-hat-build-of-keycloak) | [Token validation on the Kafka cluster](demos/08-token-validation.md) | broker-side JWKS validation |
| 8 | [§5 Cluster Migration & Upstream Switching](../README.md#5-cluster-migration--upstream-switching) | [Live migration / cutover (MM2)](demos/03-live-migration.md) | — |
| 9 | [§5 Cluster Migration & Upstream Switching](../README.md#5-cluster-migration--upstream-switching) | [MM2 failover / failback](demos/10-mm2-failover.md) | `deploy/demos/10-mm2-failover.yaml` |
| 10 | [§5 Cluster Migration & Upstream Switching](../README.md#5-cluster-migration--upstream-switching) | [Disaster recovery active/passive](demos/14-disaster-recovery.md) | — |
| 11 | [§6 Data Quality: Blocking Poison Messages](../README.md#6-data-quality-blocking-poison-messages-record-validation-filter) | [Record Validation end-to-end](demos/02-record-validation.md) | `deploy/demos/02-record-validation-demo.yaml` |
| 12 | [§6 Data Quality: Blocking Poison Messages](../README.md#6-data-quality-blocking-poison-messages-record-validation-filter) | [Schema evolution](demos/12-schema-evolution.md) | registry from Step 5.4 |
| 13 | [§7 Encryption at Rest / §8 Key Management](../README.md#7-encryption-at-rest-for-records-record-encryption-filter) | [Record Encryption at rest](demos/11-record-encryption.md) | `deploy/demos/11-record-encryption.yaml` (Vault dev) |

## The combined narrative

> (5) stable endpoints → (1) OAuth-secured data path → (1) revoke a client live → (2/12) validation/contract enforcement at the proxy → (3/14) mirror + cutover with clients unaware → (6/7/8) authorization, isolation, and token checks at the edge → (11) encryption at rest the brokers can't read → all of it visible in (15) the dashboards ([Step 1](01-user-workload-monitoring.md)).
