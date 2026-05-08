package com.example.order.resource;

import com.example.order.model.Order;
import com.example.order.model.OrderEvent;
import com.example.order.model.OrderRequest;
import com.example.order.model.ApiResponse;
import com.example.order.producer.OrderProducer;
import com.example.order.service.KafkaStatusService;
import com.example.order.service.OrderService;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.concurrent.CompletionException;

@Path("/api/orders")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OrderResource {

    private static final Logger LOG = Logger.getLogger(OrderResource.class);

    @Inject
    OrderProducer orderProducer;

    @Inject
    OrderService orderService;

    @Inject
    KafkaStatusService kafkaStatusService;

    @POST
    public Response createOrder(OrderRequest request) {
        if (request == null || request.customerId == null || request.productName == null || request.amount == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ApiResponse.error("customerId, productName, and amount are required", null))
                    .build();
        }

        if (request.amount.signum() <= 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(ApiResponse.error("amount must be greater than zero", null))
                    .build();
        }

        Order order = orderService.createPendingOrder(request);

        LOG.infof("Order saved to DB: orderId=%d, customerId=%s", order.id, order.customerId);

        OrderEvent event = new OrderEvent(
                order.id,
                order.customerId,
                order.productName,
                order.amount,
                order.status,
                order.createdAt
        );

        try {
            orderProducer.sendOrder(event).toCompletableFuture().join();
            return Response.status(Response.Status.CREATED)
                    .entity(ApiResponse.success("Order created and published to Kafka", order))
                    .build();
        } catch (CompletionException e) {
            LOG.errorf(e, "Failed to publish order to Kafka after DB commit: orderId=%d", order.id);
            Order updatedOrder = orderService.updateStatus(order.id, "PUBLISH_FAILED").orElse(order);
            return Response.status(Response.Status.ACCEPTED)
                    .entity(ApiResponse.error("Order saved to database, but failed to publish to Kafka", updatedOrder))
                    .build();
        }
    }

    @GET
    public Response getAllOrders() {
        List<Order> orders = orderService.getAllOrders();
        return Response.ok(ApiResponse.success("Orders fetched successfully", orders)).build();
    }

    @GET
    @Path("/kafka/status")
    public Response getKafkaStatus() {
        try {
            return Response.ok(ApiResponse.success("Kafka connection is healthy", kafkaStatusService.checkStatus()))
                    .build();
        } catch (Exception e) {
            LOG.error("Kafka connectivity check failed", e);
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(ApiResponse.error("Kafka connection failed: " + e.getMessage(), null))
                    .build();
        }
    }

    @GET
    @Path("/{id:\\d+}")
    public Response getOrderById(@PathParam("id") Long id) {
        return orderService.findById(id)
                .map(order -> Response.ok(ApiResponse.success("Order fetched successfully", order)).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(ApiResponse.error("Order not found", null))
                        .build());
    }
}
