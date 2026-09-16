# Bài tập 4: Xử lý lỗi cho Kafka Consumer với Retry và Dead Letter Queue (DLQ)

## Phần 1 - Phân tích

### 1. Cơ chế đọc Offset của Kafka
- **Offset**: Mỗi message trong một Kafka Partition được gán một định danh tuần tự gọi là offset, bắt đầu từ 0.
- **Consumer Offset**: Kafka Consumer theo dõi vị trí (offset) của message tiếp theo mà nó sẽ đọc từ partition. Offset này được lưu trữ trong một internal topic tên là `__consumer_offsets`.
- Khi consumer xử lý thành công một message, nó sẽ commit offset để đánh dấu rằng message đó đã được tiêu thụ thành công.

### 2. Lý do Consumer bị kẹt khi gặp Exception
- Trong đoạn code cũ, khi gặp lỗi (ví dụ chuỗi JSON sai định dạng gây ra `SerializationException` hoặc `NullPointerException`), phương thức `consume` văng ngoại lệ (`Exception`).
- Vì không có cơ chế `ErrorHandler` hay `SeekToCurrentErrorHandler` cấu hình tường minh, Spring Kafka sẽ mặc định cố gắng xử lý lại (redeliver) chính message đó tại cùng một offset liên tục vô hạn.
- Do consumer cứ lặp đi lặp lại việc đọc và xử lý message bị lỗi này mà không bao giờ commit offset thành công, nó bị "kẹt" tại đây và các message hợp lệ nằm ở các offset phía sau không bao giờ được đọc tới.

---

## Phần 2 - Giải pháp
- Sử dụng `DefaultErrorHandler` kết hợp với `FixedBackOff` để thực hiện retry tối đa 3 lần.
- Sử dụng `DeadLetterPublishingRecoverer` để chuyển message lỗi sang Dead Letter Queue (topic `order-events.DLT`) sau khi đã retry hết số lần cho phép.
- Commit offset của message lỗi để consumer tiếp tục di chuyển sang các message tiếp theo mà không bị kẹt hệ thống.