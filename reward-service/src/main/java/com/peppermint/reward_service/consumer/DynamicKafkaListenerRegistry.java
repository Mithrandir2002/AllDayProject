package com.peppermint.reward_service.consumer;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

@Component
public class DynamicKafkaListenerRegistry {

    private final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory;

    public DynamicKafkaListenerRegistry(
            ConcurrentKafkaListenerContainerFactory<String, String> containerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        this.containerFactory = containerFactory;
        this.containerFactory.setCommonErrorHandler(kafkaErrorHandler);
    }

    /**
     * Khởi tạo và chạy trực tiếp một Consumer Container
     */
    public void register(String listenerId, String topic, String groupId, MessageListener<String, String> listener) {

        // 1. Tạo container trực tiếp từ factory cho topic tương ứng
        ConcurrentMessageListenerContainer<String, String> container = containerFactory.createContainer(topic);

        // 2. Cài đặt các thông số cho luồng lắng nghe
        container.getContainerProperties().setGroupId(groupId);
        container.getContainerProperties().setMessageListener(listener);
        container.setBeanName(listenerId);

        // 3. Kích hoạt Consumer (bắt đầu lắng nghe message)
        container.start();
    }
}
