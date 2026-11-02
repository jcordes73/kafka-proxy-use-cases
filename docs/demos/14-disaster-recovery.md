# Demo 14: Disaster recovery active/passive

> [↑ Demo scenarios](../11-demos.md) · prev: [Demo 13: Custom filter](13-custom-filter.md) · next: [End-to-end demo walkthrough](../12-demos-walkthrough.md)

cluster-2 as the passive replica:

1. MM2 keeps cluster-2 in sync (Demo 3); with Demo 10's `checkpointConnector`, consumer offsets are translated too.
2. "Lose" the source: `oc scale kafka kafka-cluster-1 -n kafka-cluster-1 --replicas=0` (dev only).
3. Switch the proxy upstream to cluster-2 (`KafkaService`, Demo 3 step 7) — clients ride through on the same virtual-cluster bootstrap; consumers resume at the translated offsets (Demo 10).
4. "Recover": scale cluster-1 back, reverse-mirror, and cut back — the rollback is switching the upstream again.
