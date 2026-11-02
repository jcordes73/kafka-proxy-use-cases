# Demo 4: DMZ pivot attempt

> [↑ Demo scenarios](../10-demos.md) · prev: [Demo 3: Live migration / cutover](03-live-migration.md) · next: [Demo 5: Stable endpoints](05-stable-endpoints.md)

From [Step 3.4](../03-kafka-cluster.md#34--restrict-kafka-ingress-to-the-proxy-namespace-dmz):

```bash
# From an unrelated namespace: brokers are unreachable — TIMEOUT
oc run kcat-blocked --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -n default -- \
  -b kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9092 -L

# From kafka-proxy: the only way in is through the proxy
oc run kcat-allowed --rm -i --restart=Never --image=confluentinc/cp-kcat:latest -n kafka-proxy -- \
  -b kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9092 -L
```

The message: *treat proxy access as broker access* — the DMZ makes the proxy the single door.
