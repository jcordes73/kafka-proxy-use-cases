# deploy/camel/producer/Dockerfile — Step 14.4 (openshift-setup.md)
# Runtime-only image: the quarkus-app fast-jar is built with `mvn -B package -DskipTests`
# and copied in (with an in-cluster BuildConfig, stage target/quarkus-app alongside
# this Dockerfile and use `oc start-build --from-dir`).
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:latest
COPY target/quarkus-app /deployments/
