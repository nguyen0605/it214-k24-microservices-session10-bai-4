package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderEvent;
import com.storex.inventory.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);
    private final InventoryService inventoryService;

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = "order-events", groupId = "inventory-group")
    public void consume(OrderEvent event) {
        try {
            log.info("Received order event: {}", event);
            inventoryService.deductStock(event.getProductId(), event.getQuantity());
        } catch (Exception e) {
            log.error("Error processing order event: {}. Exception: {}", event, e.getMessage());
            // Ném lại ngoại lệ để Spring Kafka ErrorHandler bắt và kích hoạt cơ chế Retry/DLQ
            throw e;
        }
    }
}