# Chạy đồng thời ba Spark application

Docker Compose quản lý ba driver qua `spark-job-vehicle`, `spark-job-trip`,
`spark-job-alert`. Mỗi service chạy `spark-submit --deploy-mode client`, đăng ký
với `spark-master` và nhận executor trên `spark-worker`.

Worker mặc định cung cấp 3 core và 4 GB bộ nhớ cho việc cấp phát executor.
Mỗi application bị giới hạn 1 core, 1 core/executor và 1 GB executor heap;
dynamic allocation bị tắt. Ba driver có heap 1 GB/driver, chạy trong container riêng.
Các giá trị này là giới hạn cấp phát của Spark, không phải CPU/RAM quota Docker.
Cần tính thêm RAM cho Python, JVM overhead, RocksDB, Kafka và PostgreSQL.

## Khởi động

Trong thư mục gốc repo, kiểm tra `.env`:

```dotenv
SPARK_WORKER_CORES=3
SPARK_WORKER_MEMORY=4g
SPARK_DRIVER_MEMORY=1g
SPARK_EXECUTOR_MEMORY=1g
```

`.env` có thể ghi đè mặc định Compose. Tăng executor memory cần tăng worker memory
tương ứng để đủ cho cả ba application. Không chạy thêm job bằng `docker compose exec`
khi ba service đã hoạt động: driver thứ tư có thể tranh tài nguyên hoặc checkpoint.

```bash
docker compose --profile streaming config --quiet
docker compose build spark-master spark-worker
docker compose --profile streaming up -d
```

Profile `streaming` bật producer và cả ba job, cùng các service hạ tầng.
Mỗi job chờ PostgreSQL healthy, topic được `kafka-init` tạo xong và Spark sẵn sàng.
Image cần build lại khi dependency hoặc nội dung đóng gói thay đổi.
Ivy cache vẫn dùng volume bền vững; lần đầu/cache miss còn phụ thuộc Maven và mạng.

## Quan sát

```bash
docker compose --profile streaming ps
docker compose logs -f --tail 50 spark-job-vehicle spark-job-trip spark-job-alert
```

Spark Master UI: <http://localhost:8081> (hoặc `SPARK_MASTER_UI_PORT` trong `.env`).
Kiểm tra có ba application RUNNING, mỗi application **được cấp** một core.
Container running chưa chứng minh micro-batch đã hoàn thành: cần xem log, Spark UI
và dữ liệu mới trong DB. Log mỗi driver được xoay vòng, tối đa 3 file × 10 MB.

```sql
SELECT 'vehicle' AS flow, count(*), max(created_at) FROM staging.vehicle_positions
UNION ALL SELECT 'trip', count(*), max(created_at) FROM staging.trip_stop_updates
UNION ALL SELECT 'alert', count(*), max(created_at) FROM staging.service_alert_entities;
```

Alert có thể không phát sinh record mới sau dedup dù application đang hoạt động.
Ba topic được xử lý độc lập, không có transaction/batch chung cho cả ba.

## Restart và dừng

```bash
docker compose restart spark-job-trip
docker compose stop -t 60 producer spark-job-vehicle spark-job-trip spark-job-alert
docker compose stop spark-worker spark-master kafka postgres
```

Restart policy áp dụng khi process thoát; Docker không tự phát hiện stream bị treo.
`stop_grace_period: 60s` cho process thời gian thoát trước SIGKILL, không đảm bảo
micro-batch luôn hoàn thành. Checkpoint và sink idempotent xử lý việc chạy lại batch.

Ba job giữ checkpoint PostgreSQL riêng trong volume `spark-checkpoints`, dùng lại
các đường dẫn `vehicle-state-postgres-v1`, `trip-updates-postgres-v1`,
`service-alert-postgres-v1`. Có thể ghi đè qua các biến `*_CHECKPOINT_LOCATION`
trong Compose. `startingOffsets=latest` chỉ áp dụng khi chưa có checkpoint.
Nếu checkpoint cũ trỏ tới Kafka offset đã bị retention xóa, cần xử lý/replay có chủ đích;
không tự xóa checkpoint hoặc bỏ qua lỗi mất dữ liệu. Không dùng `down -v` để restart.

Đây là triển khai standalone trên một máy, chưa có HA cho worker/master.
Tham khảo cơ chế cấp phát core tại [Spark Standalone resource scheduling](https://spark.apache.org/docs/4.2.0/spark-standalone.html#resource-scheduling).

## Kiểm thử thực tế ngày 2026-09-20

- Build image, Compose validation, Ruff và 6 test cấu hình đều pass.
- Ba application đồng thời RUNNING, mỗi application được cấp 1 core/1024 MB;
  worker dùng 3/3 core và 3072/4096 MB executor memory.
- Cả ba bảng có bản ghi mới từ MBTA qua producer → Kafka → Spark → PostgreSQL.
- Restart riêng TripUpdate tạo application ID mới; Vehicle và Alert giữ nguyên ID,
  tiếp tục hoạt động. TripUpdate tiếp tục cập nhật checkpoint sau restart.

Lần chạy này phát hiện Kafka đã bắt đầu lại offset nhưng checkpoint Vehicle cũ
vẫn trỏ tới offset 23918. Bảng Vehicle cũ cũng có UNIQUE trên
`(kafka_topic, kafka_partition, kafka_offset)`, nên tái sử dụng offset với dữ liệu
cũ sẽ gây xung đột dù event mới khác nhau.

Theo xác nhận của người dùng, bảng Vehicle được chuyển vào schema
`archive_kafka_reset_20260920`, sau đó tạo bảng staging mới bằng `LIKE ... INCLUDING ALL`.
Đã xác nhận **56.183 record** còn nguyên trong bảng lưu trữ. Sau replay, bảng Vehicle
mới có **2.659 record** tại thời điểm kiểm tra; TripUpdate có **109.774 row** và Alert
có **4.465 row**. Các số này tiếp tục thay đổi khi pipeline chạy.
Đây là thao tác phục hồi dữ liệu local một lần, không phải migration chạy lúc startup.
Checkpoint cũ được giữ lại. `.env` local chọn checkpoint
`/opt/spark/checkpoints/vehicle-state-postgres-kafka-20260920`; lần đầu dùng
`KAFKA_STARTING_OFFSETS=earliest` để replay dữ liệu **còn trong Kafka**.
Không thể khôi phục message đã mất khỏi Kafka bằng checkpoint mới.
