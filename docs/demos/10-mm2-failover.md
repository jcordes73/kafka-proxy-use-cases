# Demo 10: MM2 failover / failback

> [↑ Demo scenarios](../10-demos.md) · prev: [Demo 9: Multi-tenant virtual clusters](09-multi-tenant-virtual-clusters.md) · next: [Demo 11: Record Encryption at rest](11-record-encryption.md)

The offset-translation variant of the replication:

1. Apply the failover variant (adds `checkpointConnector` with `sync.group.offsets.enabled: "true"` and `heartbeatConnector`):

   ```bash
   oc apply -f deploy/demos/10-mm2-failover.yaml
   oc wait --for=condition=Ready kafkamirrormaker2/kafka-cluster-1-to-2 -n kafka-mm2 --timeout=600s
   ```

2. Produce with a consumer group active on the source (the Camel consumer, `groupId=camel-consumer`).
3. Check the translated offsets on the target: `kafka-cluster-1.camel-consumer` in the `mm2-checkpoints.internal` topic / the console.
4. **Failover**: switch the `KafkaService` upstream (Demo 3, step 7) — the consumer resumes at the translated offset on the target, not from scratch.
5. **Failback**: switch the upstream back; reverse replication can be added by mirroring in the other direction.
6. Revert: `oc apply -f deploy/mirrormaker2/01-mirrormaker2.yaml`
