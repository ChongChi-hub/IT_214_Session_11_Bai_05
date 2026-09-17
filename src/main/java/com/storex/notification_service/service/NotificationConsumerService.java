package com.storex.notification_service.service;

import com.storex.notification_service.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class NotificationConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(NotificationConsumerService.class);

    // Idempotency check: ConcurrentHashMap
    private final ConcurrentHashMap<String, Boolean> processedOrders = new ConcurrentHashMap<>();

    @Autowired
    private WebClient webClient;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "storex-order-events", groupId = "notification-group")
    public void consumeOrderEvent(OrderEvent event) {
        String orderId = event.getOrderId();

        // Idempotency Check (BUG-06)
        if (processedOrders.putIfAbsent(orderId, true) != null) {
            logger.info("Skip duplicate processing for order: {}", orderId);
            return; // Đã xử lý thành công trước đó, bỏ qua
        }

        String userId = event.getPayload() != null ? event.getPayload().getUserId() : "unknown";

        // Non-blocking flow
        getPreference(userId)
            .flatMap(channel -> sendNotification(channel, orderId))
            .subscribe(
                result -> logger.info("Đã gửi thông báo thành công cho order: {}", orderId),
                error -> {
                    // Fallback to DLQ manually since we are not blocking the Kafka Listener thread (BUG-07)
                    logger.error("Đã đẩy order {} vào DLQ do lỗi gửi thông báo: {}", orderId, error.getMessage());
                    kafkaTemplate.send("storex-order-events.DLQ", orderId, event);
                    // Có thể remove orderId khỏi processedOrders nếu muốn cho phép retry sau này
                }
            );
    }

    private Mono<String> getPreference(String userId) {
        return webClient.get()
                .uri("http://localhost:8081/api/preferences/" + userId)
                .retrieve()
                .bodyToMono(String.class)
                .retryWhen(Retry.fixedDelay(2, Duration.ofSeconds(1)))
                .onErrorResume(e -> {
                    logger.warn("Preference API thất bại, chuyển sang Fallback EMAIL cho user {}", userId);
                    return Mono.just("EMAIL"); // Fallback
                });
    }

    private Mono<String> sendNotification(String channel, String orderId) {
        String url = channel.equalsIgnoreCase("ZALO") 
                ? "http://localhost:8082/api/notify/zalo" 
                : "http://localhost:8082/api/notify/email";

        return webClient.post()
                .uri(url)
                .bodyValue("{\"orderId\": \"" + orderId + "\"}")
                .retrieve()
                .bodyToMono(String.class)
                .retryWhen(Retry.fixedDelay(2, Duration.ofSeconds(1))); // Thử lại tối đa 2 lần
    }
}
