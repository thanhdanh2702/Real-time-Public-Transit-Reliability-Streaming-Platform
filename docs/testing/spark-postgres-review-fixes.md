# Spark review fixes — vấn đề 1, 2 và 5

## Phạm vi

- Vehicle quality kiểm tra `speed_mps >= 0`, `odometer >= 0`, `0 <= bearing <= 360`.
  Các trường này vẫn được phép `null`, giống constraint PostgreSQL.
- Alert loại phần tử `null` trong `informed_entities` và `active_periods`.
  Parser giữ `_corrupt_record` để quality phân biệt timestamp sai định dạng với
  endpoint `null` hợp lệ. Không thay đổi event schema của producer; cột kỹ thuật
  này không được đưa vào bảng PostgreSQL.
- Cả ba main dùng `postgres_connection_from_env()` trước khi tạo Spark:
  port phải hợp lệ, password phải tồn tại và không rỗng.
  Sau khi tạo session, `finally` vẫn dừng Spark nếu source/sink/query lỗi.
- Không thay đổi phân bổ tài nguyên (#3), không triển khai DLQ (#4).
  Các record sai đã được ghi trước đây không tự động được sửa/xóa.

## Test đã bổ sung

| Phạm vi | Test | Điều được kiểm tra |
|---|---|---|
| Vehicle quality | `spark/tests/unit/test_transforms.py` | Giá trị âm, bearing ngoài khoảng, giá trị biên và null |
| Alert quality | `spark/tests/unit/test_service_alert_transforms.py` | Null element, array rỗng/null, ngày sai, khoảng mở; filter và projection không bỏ kiểm tra parse lỗi |
| Cấu hình | `spark/tests/unit/test_main_configuration.py` | Không tạo Spark khi port/password lỗi, áp dụng cả ba job |
| Main | `test_vehicle_state_main.py`, `test_trip_update_main.py`, `test_service_alert_main.py` | Nối đúng từng bước, dùng valid branch, cấu hình sink/checkpoint và stop khi lỗi |
| Sink unit | `spark/tests/unit/test_postgres_sink.py` | Không bỏ row đầu, partition rỗng, SQL identifiers, JSONB, truyền lỗi DB và cấu hình foreachBatch |
| Sink integration | `spark/tests/integration/test_postgres_sink.py` | Cả ba flow ghi DB thật, replay, duplicate qua batch, restart checkpoint, lỗi sau DB commit, rollback transaction |

## Bằng chứng chạy ngày 2026-09-18

Test hồi quy trước khi sửa: **15 failed, 2 passed**, đúng các lỗi quality và
kiểm tra cấu hình quá muộn. Chạy lại sau sửa: **33 passed** cho nhóm regression,
transform và main hiện có. Các test mở rộng được thêm sau đó.

Suite mặc định: **77 passed, 7 skipped**. Bảy test integration opt-in được chạy
riêng trong Docker: **7 passed**. Coverage dòng của `spark/common` và `spark/jobs`:
**97%** (không tính các file test vào số này).

Ruff toàn repo và mypy bảy file source thay đổi đều pass; `git diff --check` pass.
Không tự tạo commit hoặc push; giữ nguyên các thay đổi có sẵn trong working tree.

### Chạy suite mặc định từ thư mục gốc repo

```bash
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python" \
.venv/bin/python -m pytest -q
```

Hai biến Python đảm bảo Spark worker dùng cùng interpreter với pytest.

### Đo riêng coverage source Spark

```bash
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python" \
.venv/bin/python -m pytest -q spark/tests tests \
  --cov-reset --cov=spark/common --cov=spark/jobs --cov-report=term-missing
```

### Chạy integration PostgreSQL trong Docker

PostgreSQL phải đang chạy, Spark image đã có `pytest` và `psycopg`, `.env` chứa
thông tin kết nối hợp lệ. Test dùng DB của Compose nhưng chỉ ghi schema
`test_spark_<uuid>` riêng, tạo từ migrations 002–004 và xóa schema đó khi kết thúc.
Không cần bật producer, Kafka hoặc Spark worker; test chạy Spark `local[2]`.

```bash
docker compose run --rm --no-deps \
  --entrypoint python3 \
  -e RUN_POSTGRES_INTEGRATION=1 \
  -e PYSPARK_PYTHON=python3 \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  -v "$PWD/database:/opt/transitpulse/database:ro" \
  -v "$PWD/pyproject.toml:/opt/transitpulse/pyproject.toml:ro" \
  spark-master -m pytest --no-cov -p no:cacheprovider -q \
  spark/tests/integration/test_postgres_sink.py
```

Test giả lập lỗi sẽ cố ý tạo log ERROR của Spark; kết quả cuối pytest mới là
tiêu chí pass/fail. Đã xác nhận không còn schema test sau lần chạy này.

## Giới hạn

- Integration dùng file stream với envelope tương đương Kafka, không gọi MBTA
  hoặc đọc Kafka thật. Đây không phải test E2E API → Kafka → Spark → PostgreSQL.
- Chưa kiểm thử load, nhiều executor, mất mạng dài hạn hoặc concurrent writers.
- Coverage cao không có nghĩa đã bao quát mọi dữ liệu đầu vào hay lỗi vận hành.
- CI mặc định skip integration nếu chưa bật flag và cung cấp PostgreSQL.
