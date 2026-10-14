package com.peppermint.reward_service.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor // Lombok tự động tạo Constructor để inject KafkaTemplate
public class KafkaProducerService {

    // Spring Boot đã tự động tạo bean này từ file application.yml, bạn chỉ cần gọi ra dùng
    // Dùng kiểu Object để có thể gửi bất kỳ class DTO nào
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Gửi message vào Kafka (Bất đồng bộ)
     *
     * @param topic Tên topic cần gửi
     * @param key   Khóa của message (giúp Kafka đảm bảo thứ tự xử lý nếu cùng 1 key)
     * @param data  Payload (Đối tượng Java, sẽ được tự động parse sang JSON)
     */
    public CompletableFuture<SendResult<String, Object>> send(String topic, String key, Object data) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, data);

        // Vẫn giữ callback này để log tập trung ở 1 nơi (đỡ phải viết log nhiều lần)
        future.whenComplete((result, exception) -> {
            if (exception == null) {
                log.info("Đã gửi message thành công tới topic: {} | Partition: {}",
                        topic, result.getRecordMetadata().partition());
            } else {
                log.error("Lỗi khi gửi message tới topic {}: {}", topic, exception.getMessage());
                // TODO: Ghi vào DB Outbox Pattern
            }
        });

        // Trả ra ngoài cho Caller tự quyết định
        return future;
    }
}
