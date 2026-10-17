package com.peppermint.reward_service.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;

@Slf4j
@Configuration
public class KafkaConsumerErrorHandlingConfig {

    /**
     * Retries a failed record in the current consumer before publishing it to
     * {@code <source-topic>.DLT}. The DLT record includes Spring Kafka headers
     * with the original topic, partition, offset, and exception details.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<?, ?> kafkaTemplate,
            @Value("${app.kafka.consumer.max-attempts:3}") long maxAttempts,
            @Value("${app.kafka.consumer.retry-interval:1s}") Duration retryInterval) {

        if (maxAttempts < 1) {
            throw new IllegalArgumentException("app.kafka.consumer.max-attempts must be at least 1");
        }

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", -1)
        );
        recoverer.setFailIfSendResultIsError(true);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(retryInterval.toMillis(), maxAttempts - 1)
        );

        // A malformed message or failed validation will not become valid on retry.
        errorHandler.addNotRetryableExceptions(
                DeserializationException.class,
                IllegalArgumentException.class
        );
        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(
                    org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                    Exception exception,
                    int deliveryAttempt) {
                log.warn("Kafka message processing failed; topic={}, partition={}, offset={}, attempt={}",
                        record.topic(), record.partition(), record.offset(), deliveryAttempt, exception);
            }

            @Override
            public void recovered(
                    org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> record,
                    Exception exception) {
                log.error("Kafka message moved to DLQ; sourceTopic={}, partition={}, offset={}, dlqTopic={}.DLT",
                        record.topic(), record.partition(), record.offset(), record.topic(), exception);
            }
        });

        return errorHandler;
    }
}
