package com.peppermint.reward_service.consumer;

import org.springframework.kafka.listener.MessageListener;

public interface KafkaTopicHandler extends MessageListener<String, String> {
    // Khai báo topic mà class này sẽ xử lý
    String getTopic();

    // Khai báo Group ID
    String getGroupId();

    // ID định danh duy nhất cho Listener
    default String getListenerId() {
        return this.getClass().getSimpleName() + "-" + getTopic();
    }
}
