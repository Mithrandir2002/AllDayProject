package com.peppermint.reward_service.consumer;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class RewardSubscriberService {

    @Autowired
    private DynamicKafkaListenerRegistry kafkaRegistry;

    // Spring tự động tìm tất cả các class implements KafkaTopicHandler (như RewardPointHandler, VoucherIssueHandler...)
    @Autowired
    private List<KafkaTopicHandler> handlers;

    @PostConstruct
    public void initConsumers() {
        // Duyệt qua danh sách và đăng ký tự động
        for (KafkaTopicHandler handler : handlers) {
            kafkaRegistry.register(
                    handler.getListenerId(),
                    handler.getTopic(),
                    handler.getGroupId(),
                    handler
            );

            System.out.println("Registered Kafka Consumer for topic: " + handler.getTopic());
        }
    }
}
