package com.example.order.model;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
public class Order extends PanacheEntity {

    @Column(nullable = false)
    public String customerId;

    @Column(nullable = false)
    public String productName;

    @Column(nullable = false)
    public BigDecimal amount;

    @Column(nullable = false)
    public String status;

    @Column(nullable = false)
    public LocalDateTime createdAt;

    public static Order findByCustomerId(String customerId) {
        return find("customerId", customerId).firstResult();
    }
}
