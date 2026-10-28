# deploy/custom-filter/Dockerfile — Step 16.4 (openshift-setup.md)
# Custom proxy image: layers the filter plugin JARs onto the stock proxy image.
# The proxy startup script scans /opt/kroxylicious/classpath-plugins/ subdirectories
# and adds their contents to the classpath (Alpha feature — see the upstream
# Developer Guide). Keep the base tag in lockstep with the operator version.
FROM quay.io/kroxylicious/proxy:0.25.1

COPY target/*.jar /opt/kroxylicious/classpath-plugins/sample-filter/
