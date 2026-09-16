# BẢN PHÂN TÍCH CHUYÊN SÂU: BUG-04 & REQ-01 (BÀI TẬP 3)

---

## 1. PHÂN TÍCH VÀ KHẮC PHỤC LỖI BUG-04 (MECHANISM FAN-OUT & CONSUMER GROUP)

### 1.1 Tình huống sự cố (Symptom)
Lập trình viên khi triển khai `Inventory-Service` (Trừ kho) và `Loyalty-Service` (Tích điểm) đã sao chép cấu hình và dùng chung thuộc tính Consumer Group ID:
```yaml
# Cấu hình sai (Lỗi BUG-04)
spring.kafka.consumer.group-id: storex-system
```

### 1.2 Nguyên nhân gốc rễ (Root Cause & Mechanism)
Trong Apache Kafka, mô hình tiêu thụ thông điệp phụ thuộc hoàn toàn vào cách thiết lập `group.id`:

1. **Competing Consumers Pattern (Mô hình cạnh tranh / Chia tải trong cùng 1 Group)**:
   - Khi nhiều Consumer Instance có cùng một `group.id`, Kafka coi chúng là các worker thuộc **cùng một ứng dụng thành phần (Logical Service)**.
   - Kafka Broker sẽ phân chia ngẫu nhiên/lần lượt các Partition của Topic cho các Consumer trong cùng nhóm.
   - **Hậu quả**: Mỗi message bắn vào Topic **chỉ được gửi cho duy nhất 1 Consumer** trong nhóm. Nếu `Inventory-Service` nhận đơn hàng #101, thì `Loyalty-Service` sẽ **KHÔNG nhận được** đơn hàng #101 đó. Đơn hàng bị trừ kho thì không được cộng điểm thưởng và ngược lại.

2. **Publish-Subscribe / Fan-out Pattern (Mô hình phát sóng / Độc lập)**:
   - Để triển khai kiến trúc Choreography Microservices, mỗi Service chuyên biệt (`Inventory-Service`, `Loyalty-Service`, `Notification-Service`, v.v.) bắt buộc phải có một `group.id` độc lập.
   - Kafka đảm bảo rằng **MỖI Consumer Group nhận được 100% bản sao dữ liệu (full stream)** từ Topic.

### 1.3 Giải pháp khắc phục chuẩn xác
Đã cập nhật file `application.yml` tách biệt `group-id` cho từng Service:

- **Inventory Service (`inventory-service/src/main/resources/application.yml`)**:
  ```yaml
  spring:
    kafka:
      consumer:
        group-id: inventory-service-group
  ```

- **Loyalty Service (`loyalty-service/src/main/resources/application.yml`)**:
  ```yaml
  spring:
    kafka:
      consumer:
        group-id: loyalty-service-group
  ```

---

## 2. PHÂN TÍCH THIẾT KẾ PARTITION & SCALE-UP (REQ-01)

### 2.1 Yêu cầu nghiệp vụ
Khởi chạy 3 instance độc lập của `Inventory-Service` thuộc nhóm `inventory-service-group` để xử lý tải cao trong ngày Mega Sale (10,000 đơn/giây). Yêu cầu đề xuất số lượng Partition tối thiểu của Topic để 3 instance chia tải hiệu quả nhất (mỗi instance xử lý ~33% lượng đơn).

### 2.2 Nguyên lý phân bổ Partition của Kafka (Consumer Partition Assignment)
Kafka áp dụng quy tắc phân bổ Partition trong 1 Consumer Group như sau:
1. Một Partition tại một thời điểm **chỉ được gán cho duy nhất 1 Consumer Instance** trong cùng nhóm.
2. Một Consumer Instance có thể gánh trách nhiệm xử lý **1 hoặc nhiều Partitions**.

Từ quy tắc này, ta có mối quan hệ giữa **Số lượng Partition ($P$)** và **Số lượng Consumer Instance ($C$)**:

- **Nếu $P < C$ (Ví dụ: Topic có 2 Partitions, chạy 3 Instance)**:
  - Instance 1 được gán Partition 0.
  - Instance 2 được gán Partition 1.
  - **Instance 3 rơi vào trạng thái Nhàn rỗi (IDLE - 0 Partition)**, gây lãng phí tài nguyên server do không bao giờ nhận được message.
- **Nếu $P \ge C$ (Ví dụ: Topic có 5 Partitions, chạy 3 Instance)**:
  - Cả 3 Instance đều được gán ít nhất 1 Partition và làm việc song song 100%.

### 2.3 Đề xuất & Đánh giá với Topic `storex-order-events`

1. **Số lượng Partition tối thiểu cần có**: **3 Partitions**.
   - Với 3 Partitions và 3 Instance `Inventory-Service`, tỉ lệ phân chia là $1:1$ (mỗi instance xử lý đúng 33.33% lượng đơn hàng).

2. **Đánh giá thực tế với Topic `storex-order-events` (được cấu hình 5 Partitions từ Bài 2)**:
   - Topic `storex-order-events` có **5 Partitions** (P0, P1, P2, P3, P4).
   - Khi mở rộng `Inventory-Service` thành 3 Instances:
     - **Instance 1**: Nhận Partition P0, P1 (~40% tải)
     - **Instance 2**: Nhận Partition P2, P3 (~40% tải)
     - **Instance 3**: Nhận Partition P4 (~20% tải)
   - **Kết luận**: Cả 3 Instance đều hoạt động với công suất tối ưu (~33% trung bình), không có instance nào bị nhàn rỗi. Đồng thời cấu hình 5 Partitions này cho phép hệ thống có thể mở rộng (Scale-out) lên tới **tối đa 5 Instances** mà không cần phải repartition lại Kafka Topic!
