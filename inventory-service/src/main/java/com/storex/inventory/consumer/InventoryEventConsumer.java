package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class InventoryEventConsumer {

    /**
     * Consumer lắng nghe sự kiện order.created từ topic 'storex-order-events'.
     * Group ID: inventory-service-group (Khắc phục BUG-04).
     * 
     * Khi scale-up lên 3 instances, Kafka Broker tự động rebalance 5 partitions của topic
     * cho 3 instances này (ví dụ: Inst 1 -> P0, P1 | Inst 2 -> P2, P3 | Inst 3 -> P4).
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

        log.info("[INVENTORY-SERVICE] Received Order Created Event!");
        log.info(" Details -> OrderID: {}, Key: {}, Partition: {}, Offset: {}", event.getOrderId(), key, partition, offset);

        // Logic xử lý trừ kho sản phẩm
        if (event.getItems() != null) {
            event.getItems().forEach(item -> 
                log.info(" Deducting stock for Product ID: {}, Quantity: {}", item.getProductId(), item.getQuantity())
            );
        }
        log.info("[INVENTORY-SERVICE] Successfully processed stock deduction for OrderID: {}\n", event.getOrderId());
    }
}
