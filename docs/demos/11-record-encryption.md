# Demo 11: Record Encryption at rest

> [↑ Demo scenarios](../11-demos.md) · prev: [Demo 10: MM2 failover / failback](10-mm2-failover.md) · next: [Demo 12: Schema evolution](12-schema-evolution.md)

Envelope encryption at the proxy with a dev-mode Vault as KMS — **the wow-factor demo**:

1. Apply the demo manifest (Vault dev + Transit KEKs + RecordEncryption filter):

   ```bash
   oc apply -f deploy/demos/11-record-encryption.yaml
   oc wait --for=condition=Ready pod -l app=vault -n kafka-vault --timeout=120s
   oc wait --for=condition=Complete job/kek-setup -n kafka-vault --timeout=180s
   oc logs job/kek-setup -n kafka-vault    # KEK-topic-A created
   oc wait --for=condition=Accepted=True virtualkafkacluster/kafka-cluster-1 -n kafka-proxy --timeout=120s
   ```

2. Produce a message through the proxy (Demo 1 client) — the producer sees plaintext; it has **no keys and no knowledge** of the encryption.
3. Consume **directly from the broker** (bypassing the proxy — from `kafka-proxy` or the cluster's namespace, Demo 4 rules) — **ciphertext**:

   ```bash
   oc run kafka-cli --rm -i --restart=Never --image=quay.io/strimzi/kafka:latest-kafka-4.2.2 -n kafka-proxy -- \
     bash -c 'echo "Y2xpZW50cw==" >/dev/null; bin/kafka-console-consumer.sh \
       --bootstrap-server kafka-cluster-1-kafka-bootstrap.kafka-cluster-1.svc.cluster.local:9092 \
       --topic topic-A --from-beginning --timeout-ms 10000 \
       --consumer-property security.protocol=PLAINTEXT'
   # -> ciphertext records (kroxylicious envelope-encrypted values)
   ```

4. Consume through the proxy again — plaintext: only the proxy holds decryption capability.
5. Topics **without** a KEK (`unresolvedKeyPolicy: PASSTHROUGH_UNENCRYPTED`) pass through unencrypted; switch to `REJECT` so unencrypted data is never forwarded.
6. Revert: `oc apply -f deploy/proxy/05-virtualkafkacluster.yaml`, then `oc delete namespace kafka-vault`

> Vault runs in **dev mode** (in-memory, fixed root token) — DEV ONLY. Production: a real Vault with the Transit engine, a least-privilege periodic token, and TLS ([Red Hat 3.2 Record Encryption guide](https://docs.redhat.com/en/documentation/red_hat_streams_for_apache_kafka/3.2/html/deploying_and_managing_streams_for_apache_kafka_on_openshift)).
