package com.storex.notification_service.dto;

import lombok.Data;
import java.util.List;

@Data
public class OrderEvent {
    private String orderId;
    private OrderRequest payload;
    private String status;

    @Data
    public static class OrderRequest {
        private String userId;
        private List<String> productIds;
        private double totalAmount;
    }
}
