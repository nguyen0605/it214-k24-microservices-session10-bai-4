package com.storex.inventory.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<String, Object> template) {
        // Cấu hình Dead Letter Recoverer để chuyển message lỗi sang topic DLQ
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        
        // Retry tối đa 3 lần, mỗi lần cách nhau 1 giây
        FixedBackOff backOff = new FixedBackOff(1000L, 3L);
        
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        
        // Có thể cấu hình bỏ qua một số exception không cần retry (ví dụ: lỗi cú pháp nghiêm trọng)
        // errorHandler.addNotRetriedException(IllegalArgumentException.class);
        
        return errorHandler;
    }
}