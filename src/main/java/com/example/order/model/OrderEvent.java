package com.example.order.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class OrderEvent {

    public Long orderId;
    public String customerId;
    public String productName;
    public BigDecimal amount;
    public String status;
    public String priority;
    public LocalDateTime createdAt;
    public int retryCount;

    public OrderEvent() {}

    public OrderEvent(Long orderId, String customerId, String productName,
                      BigDecimal amount, String status, LocalDateTime createdAt) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productName = productName;
        this.amount = amount;
        this.status = status;
        this.createdAt = createdAt;
        this.retryCount = 0;
    }
}
