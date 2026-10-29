// deploy/camel/producer/src/main/java/org/example/KafkaProducerRoute.java — Step 14.2
package org.example;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;

@ApplicationScoped
public class KafkaProducerRoute extends RouteBuilder {
    @Override
    public void configure() {
        // Kafka 4.2 native OAuth: camel's saslAuthType=OAUTH neither sets the
        // client login callback handler (Kafka's default *unsecured* handler
        // mints a dummy JWT and fails with "No principal name in JWT claim:
        // sub") nor exposes sasl.oauthbearer.token.endpoint.url as a client
        // config property. These keys contain dots, so they cannot be bound
        // from application.properties via additionalProperties (Camel's
        // property binder splits on dots) — set them programmatically here.
        KafkaComponent kafka = getContext().getComponent("kafka", KafkaComponent.class);
        if (kafka.getConfiguration() == null) {
            kafka.setConfiguration(new org.apache.camel.component.kafka.KafkaConfiguration());
        }
        kafka.getConfiguration().getAdditionalProperties().putAll(Map.of(
            "sasl.login.callback.handler.class",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler",
            "sasl.oauthbearer.token.endpoint.url",
                "http://keycloak-http.kafka-auth.svc.cluster.local:8080/realms/kafka/protocol/openid-connect/token"));

        from("timer:tick?period=5s")
            .setBody(simple("hello from Camel ${date:now:yyyy-MM-dd HH:mm:ss}"))
            .to("kafka:test-topic");   // produced THROUGH the proxy
    }
}
