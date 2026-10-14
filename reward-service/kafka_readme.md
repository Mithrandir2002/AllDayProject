# Kafka Consumer: Retry và Dead Letter Queue

Tài liệu này mô tả cơ chế xử lý lỗi của Kafka consumer trong `reward-service`.

## Luồng xử lý message

Mỗi class implement `KafkaTopicHandler` được Spring tự tìm khi ứng dụng khởi động.
`RewardSubscriberService` đăng ký handler với `DynamicKafkaListenerRegistry`, sau đó registry tạo và chạy một Kafka consumer container cho topic của handler đó.

Các topic hiện tại:

| Handler | Topic | Consumer group |
| --- | --- | --- |
| `RewardPointHandler` | `add-reward-point-topic` | `reward-service-group` |
| `VoucherIssueHandler` | `issue-voucher-topic` | `reward-service-group` |

Luồng của một message:

```text
Kafka topic
    |
    v
Kafka consumer container
    |
    v
KafkaTopicHandler.onMessage(record)
    |
    +-- Xử lý thành công --> Commit offset --> Nhận message tiếp theo
    |
    +-- Ném exception --> Retry / DLQ
```

## Retry

Khi `onMessage(...)` ném exception, `DefaultErrorHandler` sẽ retry lại chính message đó.

| Cấu hình | Giá trị hiện tại | Ý nghĩa |
| --- | --- | --- |
| `app.kafka.consumer.max-attempts` | `3` | Tổng số lần xử lý: lần đầu và 2 lần retry |
| `app.kafka.consumer.retry-interval` | `1s` | Khoảng chờ giữa các lần retry |

Ví dụ message bị lỗi:

```text
Lần 1: Xử lý message -> lỗi
Chờ 1 giây
Lần 2: Xử lý lại -> lỗi
Chờ 1 giây
Lần 3: Xử lý lại -> lỗi
Chuyển message vào DLQ
```

Retry chỉ xảy ra khi handler ném exception. Ví dụ:

```java
@Override
public void onMessage(ConsumerRecord<String, String> record) {
    try {
        // Xử lý nghiệp vụ.
    } catch (Exception exception) {
        throw new RuntimeException("Cannot process reward event", exception);
    }
}
```

Không nên bắt exception rồi chỉ ghi log, vì consumer sẽ xem message là đã xử lý thành công và không retry.

## Dead Letter Queue (DLQ)

Sau khi hết số lần retry, consumer gửi message thất bại sang topic có hậu tố `.DLT`.

| Topic gốc | Topic DLQ |
| --- | --- |
| `add-reward-point-topic` | `add-reward-point-topic.DLT` |
| `issue-voucher-topic` | `issue-voucher-topic.DLT` |

DLQ được publish bởi `DeadLetterPublishingRecoverer`. Message DLQ vẫn giữ key và payload gốc, đồng thời Spring Kafka thêm headers để điều tra lỗi, gồm:

| Header | Ý nghĩa |
| --- | --- |
| `kafka_dlt-original-topic` | Topic ban đầu |
| `kafka_dlt-original-partition` | Partition ban đầu |
| `kafka_dlt-original-offset` | Offset ban đầu |
| `kafka_dlt-exception-fqcn` | Tên đầy đủ của class exception |
| `kafka_dlt-exception-message` | Nội dung lỗi |
| `kafka_dlt-exception-stacktrace` | Stack trace tại thời điểm lỗi |

Tên header có thể có tiền tố `kafka_dlt-` hoặc `kafka_dlt-` được Spring Kafka biểu diễn dưới dạng constant nội bộ, tùy phiên bản thư viện. Khi kiểm tra message, hãy xem toàn bộ headers của record thay vì phụ thuộc vào một tên cứng.

Offset của topic gốc được xử lý sau khi publish DLQ thành công, nên consumer tiếp tục đọc message kế tiếp. Nếu Kafka không thể ghi message vào DLQ, message gốc không được xem là đã phục hồi và consumer sẽ không bỏ qua nó.

## Lỗi không retry

Hai loại lỗi sau được chuyển thẳng vào DLQ:

| Exception | Lý do |
| --- | --- |
| `DeserializationException` | Payload không thể deserialize; retry không làm dữ liệu hợp lệ hơn |
| `IllegalArgumentException` | Dữ liệu không hợp lệ theo validation; cần sửa hoặc loại bỏ message |

Các exception khác, chẳng hạn lỗi database tạm thời hoặc lỗi gọi service ngoài, sẽ được retry theo cấu hình.

## Vận hành DLQ

1. Bảo đảm các topic `.DLT` tồn tại. Nếu Kafka tắt tự tạo topic, cần tạo trước các topic này.
2. Theo dõi log `Kafka message moved to DLQ` để phát hiện message lỗi.
3. Đọc payload và headers trong DLQ để xác định nguyên nhân.
4. Sửa dữ liệu hoặc lỗi nghiệp vụ.
5. Chỉ publish lại message về topic gốc sau khi đã xác định an toàn để xử lý lại.

Không nên đưa toàn bộ DLQ trở lại topic gốc tự động, vì một message lỗi dữ liệu có thể lặp vô hạn giữa topic gốc và DLQ.

## File liên quan

| File | Trách nhiệm |
| --- | --- |
| `src/main/java/com/peppermint/reward_service/config/KafkaConsumerErrorHandlingConfig.java` | Khai báo retry, DLQ recoverer và retry logs |
| `src/main/java/com/peppermint/reward_service/consumer/DynamicKafkaListenerRegistry.java` | Gắn error handler vào container tạo động |
| `src/main/resources/application.yaml` | Cấu hình số lần retry và khoảng chờ |
