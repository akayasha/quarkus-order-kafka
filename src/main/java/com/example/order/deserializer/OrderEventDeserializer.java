    package com.example.order.deserializer;

    import com.example.order.model.OrderEvent;
    import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

    public class OrderEventDeserializer extends ObjectMapperDeserializer<OrderEvent> {
        public OrderEventDeserializer() {
            super(OrderEvent.class);
        }
    }
