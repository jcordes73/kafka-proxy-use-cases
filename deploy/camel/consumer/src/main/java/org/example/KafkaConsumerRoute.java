// deploy/camel/consumer/src/main/java/org/example/KafkaConsumerRoute.java — Step 14.2
package org.example;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class KafkaConsumerRoute extends RouteBuilder {
    @Override
    public void configure() {
        from("kafka:test-topic?groupId=camel-consumer&autoOffsetReset=earliest")
            .log("received: ${body}");   // consumed THROUGH the proxy
    }
}
