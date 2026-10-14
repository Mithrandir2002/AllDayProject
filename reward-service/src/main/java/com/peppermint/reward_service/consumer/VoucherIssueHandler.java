package com.peppermint.reward_service.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

@Component
public class VoucherIssueHandler implements KafkaTopicHandler {

    @Override
    public String getTopic() {
        return "issue-voucher-topic";
    }

    @Override
    public String getGroupId() {
        return "reward-service-group";
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> record) {
        System.out.println("Processing voucher issue for message: " + record.value());

    }
}
