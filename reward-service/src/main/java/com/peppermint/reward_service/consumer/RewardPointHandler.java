package com.peppermint.reward_service.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;


@Component
public class RewardPointHandler implements KafkaTopicHandler{
    @Override
    public String getTopic() {
        return "add-reward-point-topic";
    }

    @Override
    public String getGroupId() {
        return "reward-service-group";
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> data) {
        System.out.println("Processing reward points for message: " + data.value());
    }
}
