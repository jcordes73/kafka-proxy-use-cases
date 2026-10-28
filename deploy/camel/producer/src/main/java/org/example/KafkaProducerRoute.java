// deploy/camel/producer/src/main/java/org/example/KafkaProducerRoute.java — Step 14.2
package org.example;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class KafkaProducerRoute extends RouteBuilder {
    @Override
    public void configure() {
        from("timer:tick?period=5s")
            .setBody(simple("hello from Camel ${date:now:yyyy-MM-dd HH:mm:ss}"))
            .to("kafka:test-topic");   // produced THROUGH the proxy
    }
}
