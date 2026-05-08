package com.example.order.consumer;

import com.example.order.model.OrderEvent;
import com.example.order.producer.OrderProducer;
import com.example.order.service.OrderService;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import java.math.BigDecimal;

@ApplicationScoped
public class OrderConsumer {

    private static final Logger LOG = Logger.getLogger(OrderConsumer.class);

    private static final BigDecimal HIGH_PRIORITY_THRESHOLD = new BigDecimal("1000000");

    private static final int MAX_RETRY = 3;

    @Inject
    OrderProducer orderProducer;

    @Inject
    OrderService orderService;

    @Inject
    @Channel("orders-processed-out")
    Emitter<Record<String, OrderEvent>> processedEmitter;

    @Incoming("orders-in")
    @Blocking
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public void consumeOrder(Record<String, OrderEvent> record) {
        OrderEvent event = record.value();

        if (event == null) {
            LOG.warn("Received null event. Skipping to avoid NullPointerException.");
            return;
        }

        LOG.infof("Received order from Kafka: orderId=%d, product=%s, retryCount=%d",
                event.orderId, event.productName, event.retryCount);

        try {
            processOrder(event);
        } catch (Throwable e) {
            LOG.errorf("Error processing order orderId=%d: %s", event.orderId, e.getMessage());
            handleRetry(event);
        }
    }

    private void processOrder(OrderEvent event) {

        if ("ERROR-DLQ".equalsIgnoreCase(event.productName)) {
            throw new RuntimeException("Simulated processing error triggered via request body!");
        }

        String priority = event.amount.compareTo(HIGH_PRIORITY_THRESHOLD) >= 0 ? "HIGH" : "NORMAL";
        event.priority = priority;
        event.status = "PROCESSED";

        orderService.updateStatus(event.orderId, "PROCESSED");

        processedEmitter.send(Record.of(String.valueOf(event.orderId), event))
                .toCompletableFuture()
                .join();

        LOG.infof("Order successfully PROCESSED: orderId=%d, priority=%s", event.orderId, priority);
    }

    private void handleRetry(OrderEvent event) {
        event.retryCount += 1;

        if (event.retryCount > MAX_RETRY) {
            // Jika sudah melebihi batas maksimal, lempar ke DLQ
            LOG.warnf("Max retry reached for orderId=%d. Routing to DLQ.", event.orderId);

            // Update status di database menjadi FAILED
            orderService.updateStatus(event.orderId, "FAILED");
            event.status = "FAILED";

            try {
                // Publish ke topic orders-dlq
                orderProducer.sendToDlq(event).toCompletableFuture().join();
            } catch (Exception e) {
                LOG.error("Failed to push to DLQ", e);
            }
        } else {
            LOG.infof("Retrying order orderId=%d, attempt %d of %d", event.orderId, event.retryCount, MAX_RETRY);
            try {
                orderProducer.sendOrder(event).toCompletableFuture().join();
            } catch (Exception e) {
                LOG.error("Failed to push back for retry", e);
            }
        }
    }
}