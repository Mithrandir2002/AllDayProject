package com.peppermint.reward_service.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public class RewardPointHandler implements KafkaTopicHandler{
    private final ObjectMapper objectMapper;

    public RewardPointHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

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
        //TODO dùng object mapper parse event ra RecordEvents, dùng try catch để gặp exception thì ném event vào DLQ
        System.out.println("Processing reward points for message: " + data.value());
    }
}
