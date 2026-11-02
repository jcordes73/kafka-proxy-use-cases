# Demo 3: Live migration / cutover (MM2)

> [↑ Demo scenarios](../10-demos.md) · prev: [Demo 2: Record Validation end-to-end](02-record-validation.md) · next: [Demo 4: DMZ pivot attempt](04-dmz-pivot.md)

The migration pattern from use case [5 in the guide](../../README.md#5-cluster-migration--upstream-switching):

1. Produce to `topic-A` via the proxy — lands on `kafka-cluster-1`.
2. Watch it mirror: `kafka-cluster-1.topic-A` appears on cluster-2 ([Step 4.2](../04-mirrormaker2.md#42--verify-replication)).
3. Cutover — one change, in one place (repoint the virtual cluster's `KafkaService`):

   ```bash
   oc -n kafka-proxy patch virtualkafkacluster kafka-cluster-1 --type=merge \
     -p '{"spec":{"targetKafkaServiceRef":{"name":"kafka-cluster-2"}}}'
   ```

   Clients keep using the same virtual-cluster bootstrap — no client change.
4. Rollback is re-applying the manifest wiring ([`deploy/proxy/05-virtualkafkacluster.yaml`](../../deploy/proxy/05-virtualkafkacluster.yaml)) or patching `targetKafkaServiceRef` back to `kafka-cluster-1`.
