package com.example.order.service;

import com.example.order.model.Order;
import com.example.order.model.OrderRequest;
import com.example.order.repository.OrderRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class OrderService {

    @Inject
    OrderRepository orderRepository;

    @Transactional
    public Order createPendingOrder(OrderRequest request) {
        Order order = new Order();
        order.customerId = request.customerId;
        order.productName = request.productName;
        order.amount = request.amount;
        order.status = "PENDING";
        order.createdAt = LocalDateTime.now();

        orderRepository.persist(order);
        orderRepository.flush();
        return order;
    }

    public List<Order> getAllOrders() {
        return orderRepository.listAll();
    }

    public Optional<Order> findById(Long id) {
        return orderRepository.findByIdOptional(id);
    }

    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public Optional<Order> updateStatus(Long orderId, String status) {
        Optional<Order> orderOptional = orderRepository.findByIdOptional(orderId);
        orderOptional.ifPresent(order -> order.status = status);
        return orderOptional;
    }
}
