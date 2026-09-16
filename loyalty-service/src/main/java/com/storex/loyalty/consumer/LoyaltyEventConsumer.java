package com.storex.loyalty.consumer;

import com.storex.loyalty.model.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
@Slf4j
public class LoyaltyEventConsumer {

    /**
     * Consumer lắng nghe sự kiện order.created từ topic 'storex-order-events'.
     * Group ID: loyalty-service-group (Khắc phục BUG-04).
     * 
     * Nhờ group ID độc lập, Loyalty Service nhận được 100% sự kiện song song với Inventory Service (Fan-out Pattern).
     */
    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void handleOrderCreatedEvent(
            @Payload OrderCreatedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        log.info("[LOYALTY-SERVICE] Received Order Created Event!");
        log.info(" Details -> OrderID: {}, CustomerID: {}, Key: {}, Partition: {}, Offset: {}", 
                event.getOrderId(), event.getCustomerId(), key, partition, offset);

        // Logic tính toán tích điểm (Ví dụ: 10,000 VND = 1 điểm)
        if (event.getTotalAmount() != null) {
            long rewardPoints = event.getTotalAmount().divide(new BigDecimal("10000")).longValue();
            log.info(" Awarded {} reward points to CustomerID: {} for OrderID: {}", 
                    rewardPoints, event.getCustomerId(), event.getOrderId());
        }
        log.info("[LOYALTY-SERVICE] Successfully processed reward points for OrderID: {}\n", event.getOrderId());
    }
}
