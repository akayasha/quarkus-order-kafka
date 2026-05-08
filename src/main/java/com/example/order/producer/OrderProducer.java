package com.example.order.producer;

import com.example.order.model.OrderEvent;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class OrderProducer {

    private static final Logger LOG = Logger.getLogger(OrderProducer.class);

    @Inject
    @Channel("orders-out")
    Emitter<Record<String, OrderEvent>> ordersEmitter;

    @Inject
    @Channel("orders-dlq-out")
    Emitter<Record<String, OrderEvent>> dlqEmitter;

    public CompletionStage<Void> sendOrder(OrderEvent event) {
        LOG.infof("Sending order to topic 'orders': orderId=%d, customerId=%s",
                event.orderId, event.customerId);
        return ordersEmitter.send(Record.of(String.valueOf(event.orderId), event))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOG.errorf(throwable, "Failed to send order to topic 'orders': orderId=%d",
                                event.orderId);
                    } else {
                        LOG.infof("Order successfully published to topic 'orders': orderId=%d",
                                event.orderId);
                    }
                });
    }

    public CompletionStage<Void> sendToDlq(OrderEvent event) {
        LOG.warnf("Sending failed order to DLQ: orderId=%d, retryCount=%d",
                event.orderId, event.retryCount);
        return dlqEmitter.send(Record.of(String.valueOf(event.orderId), event))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOG.errorf(throwable, "Failed to send order to topic 'orders-dlq': orderId=%d",
                                event.orderId);
                    } else {
                        LOG.infof("Order successfully published to topic 'orders-dlq': orderId=%d",
                                event.orderId);
                    }
                });
    }
}
