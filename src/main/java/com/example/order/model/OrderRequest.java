package com.example.order.model;

import java.math.BigDecimal;

public class OrderRequest {

    public String customerId;
    public String productName;
    public BigDecimal amount;

    public OrderRequest() {}

    public OrderRequest(String customerId, String productName, BigDecimal amount) {
        this.customerId = customerId;
        this.productName = productName;
        this.amount = amount;
    }
}
