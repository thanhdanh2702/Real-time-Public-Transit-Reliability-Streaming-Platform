# Nạp GTFS tĩnh vào Postgres

## Phạm vi

Loader dành cho bộ **MBTA GTFS ZIP** có sáu file ở gốc ZIP và `feed_info.txt`.
Không tải API, không cần Kafka/Spark application, không chạy dbt.
Chỉ giữ **một bộ lịch trình hiện hành**, chưa làm lịch sử phiên bản/SCD2.
Giữ ZIP gốc để có thể truy lại các cột không nạp vào database.

## Các file và trách nhiệm

- `database/migrations/005_create_gtfs_static_tables.sql`: tạo sáu bảng nguồn.
- `scripts/load_gtfs_static.py`: đọc ZIP, COPY vào bảng tạm, kiểm tra, thay dữ liệu.
- `tests/test_gtfs_static_loader.py`: test parser, nạp lại, rollback, tham chiếu.
- `docker-compose.yml`: mount migration 005 cho database khởi tạo mới.
- `pyproject.toml`: extra `gtfs` chỉ cần `psycopg[binary]` nếu chạy Python local.

| Bảng trong schema raw | Một dòng đại diện cho | Khóa chính |
|---|---|---|
| gtfs_routes | Một tuyến | route_id |
| gtfs_stops | Một điểm dừng/nhà ga | stop_id |
| gtfs_trips | Một chuyến trong lịch trình | trip_id |
| gtfs_stop_times | Một lần ghé điểm dừng trong chuyến | trip_id, stop_sequence |
| gtfs_calendar | Lịch hoạt động theo thứ của service | service_id |
| gtfs_calendar_dates | Ngoại lệ cho một ngày của service | service_id, date |

ID dùng TEXT, kể cả ID có vẻ là số, để không mất số 0 đầu.
Ngày dùng DATE; giờ đến/đi dùng TEXT vì GTFS cho phép `25:10:00`.
Các cột bổ sung `feed_version`, `loaded_at` giúp xác định nguồn và lần nạp.
Khóa chính tự tạo index; chưa thêm các index phục vụ dashboard.
Các quan hệ được kiểm tra trong loader, chưa tạo foreign key ở tầng raw.
Đây là tập cột phục vụ project, không phải toàn bộ các extension của MBTA.

## Flow của loader

1. Kiểm tra kích thước giải nén tối đa 1 GB và metadata `feed_info`.
2. Cảnh báo nếu feed hết hạn. Vẫn cho nạp để kiểm thử; không coi đó là lịch hiện tại.
3. Mở một transaction và khóa logic để hai loader cùng schema không chạy chồng.
4. Với từng file, tạo bảng tạm giống bảng đích, đọc CSV từng dòng và COPY vào đó.
   Không giải nén ra ổ đĩa, không đưa toàn bộ file vào RAM.
5. Kiểm tra header, số cột, kiểu dữ liệu, CHECK và khóa chính. Cột optional thiếu
   được nạp NULL; cột ngoài danh sách được bỏ qua. Bốn bảng tuyến/điểm/chuyến/giờ
   dừng không được rỗng; hai bảng calendar có thể rỗng nếu tham chiếu vẫn hợp lệ.
6. Kiểm tra trip → route, stop_times → trip/stop, stop → parent_station,
   trip → service trong calendar hoặc ngày được thêm bởi calendar_dates.
7. DELETE rồi INSERT vào đúng sáu bảng `raw.gtfs_*`, sau đó commit một lần.

Nếu bất kỳ bước nào thất bại, transaction rollback: bộ dữ liệu trước đó vẫn còn.
Chạy lại không cộng dồn dòng; `loaded_at` thay đổi theo lần nạp. Không xóa bảng
staging realtime, không thay checkpoint, không tác động Kafka.
DELETE giữ nguyên bảng và các view phụ thuộc; các lần refresh lớn vẫn sinh WAL và
dead tuples cần autovacuum. Mô hình này phù hợp refresh định kỳ, không chạy liên tục.
Một truy vấn SQL thấy snapshot nhất quán; nhiều truy vấn liên tiếp có thể nằm ở
hai phía của thời điểm commit. Khi cần đọc nhiều bảng cùng phiên bản, dùng transaction
REPEATABLE READ hoặc kiểm tra `feed_version`.

## Cách chạy (từ thư mục project)

```bash
cd /Users/thanhdanh/transitpulse
docker compose up -d postgres
```

Database dùng volume cũ **không tự chạy lại** các script init. Áp dụng migration
một lần bằng lệnh dưới; `CREATE TABLE IF NOT EXISTS` cho phép chạy lại:

```bash
docker compose exec -T postgres sh -c \
  'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
  < database/migrations/005_create_gtfs_static_tables.sql
```

Nạp ZIP hiện có:

```bash
docker compose run --rm --no-deps --entrypoint python3 \
  -v "$PWD/scripts:/opt/transitpulse/scripts:ro" \
  -v "$PWD/data:/opt/transitpulse/data:ro" \
  spark-master \
  /opt/transitpulse/scripts/load_gtfs_static.py \
  /opt/transitpulse/data/raw/gtfs_static/2026-08-03_version-D/MBTA_GTFS.zip
```

Lệnh này tái sử dụng image Spark **đã có psycopg**, nhưng chỉ chạy Python trong
container dùng một lần; không chạy SparkSession hay Spark master, không tải JAR.
Compose cung cấp thông tin Postgres từ `.env`, không ghi password trong code.
`--no-deps` không bật các service khác. Sau này có thể tách image Python nhỏ riêng.

Khi có ZIP mới, đặt trong `data/raw/gtfs_static/<version>/` và thay đường dẫn cuối.
Không cần đổi code theo phiên bản. Loader không tự áp dụng migration.

## Xem kết quả

```bash
docker compose exec postgres sh -c 'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Trong psql:

```sql
\dt raw.gtfs_*
SELECT * FROM raw.gtfs_routes LIMIT 5;
SELECT feed_version, loaded_at FROM raw.gtfs_routes LIMIT 1;
SELECT COUNT(*) FROM raw.gtfs_stop_times;

SELECT t.trip_id, r.route_long_name, s.stop_name,
       st.stop_sequence, st.arrival_time
FROM raw.gtfs_stop_times st
JOIN raw.gtfs_trips t USING (trip_id)
JOIN raw.gtfs_routes r USING (route_id)
JOIN raw.gtfs_stops s USING (stop_id)
LIMIT 10;
```

## Kiểm thử đã thực hiện — 2026-09-21

TDD theo nhu cầu: người dùng nạp một ZIP hoàn chỉnh, chạy lại an toàn, không mất
bộ cũ khi file lỗi. RED: 4 test thất bại do chưa có module loader; 5 integration
test chưa bật. Sau triển khai và bổ sung case: **17 test pass trên Postgres thật**,
coverage loader **97%**. Tests chỉ tạo/xóa schema tạm `test_gtfs_<uuid>`.
Bộ test chung: `PYSPARK_PYTHON="$PWD/.venv/bin/python" PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python" .venv/bin/python -m pytest -q --no-cov`
đạt 88 passed, 17 integration tests skipped khi không bật cờ; 10 GTFS integration
tests trong số đó đã chạy riêng thành công ở lệnh bên dưới. Ruff toàn repo sạch,
Compose config hợp lệ. pip-audit không phát hiện lỗ hổng đã biết; bỏ qua package
local `transitpulse` vì không có trên PyPI.

```bash
docker compose run --rm --no-deps --entrypoint python3 \
  -e RUN_POSTGRES_INTEGRATION=1 -e COVERAGE_FILE=/tmp/gtfs.coverage \
  -v "$PWD/scripts:/opt/transitpulse/scripts:ro" \
  -v "$PWD/database:/opt/transitpulse/database:ro" \
  -v "$PWD/tests:/opt/transitpulse/tests:ro" \
  -v "$PWD/pyproject.toml:/opt/transitpulse/pyproject.toml:ro" \
  --workdir /opt/transitpulse spark-master \
  -m pytest -o addopts='' -p no:cacheprovider \
  --cov=scripts.load_gtfs_static --cov-report=term-missing \
  --cov-fail-under=80 -q tests/test_gtfs_static_loader.py
```

Kiểm tra chính: ID/giờ được giữ nguyên; CSV sai header/số cột bị từ chối; thiếu
file, khóa trùng, tham chiếu sai không để dữ liệu dở dang; replay không nhân đôi;
CHECK lỗi ở file cuối giữ nguyên cả dữ liệu và loaded_at của lần nạp trước;
service chỉ xuất hiện trong calendar_dates với exception_type=1 được chấp nhận.
Chưa test chạy hai loader cạnh tranh hoặc ngắt process giữa transaction.
Hai dòng chưa cover: giới hạn ZIP 1 GB và guard gọi CLI; CLI đã chạy với ZIP thật.

Nạp ZIP MBTA thật thành công:

| Bảng | Dòng |
|---|---:|
| gtfs_routes | 400 |
| gtfs_stops | 10.298 |
| gtfs_trips | 88.438 |
| gtfs_stop_times | 2.237.146 |
| gtfs_calendar | 159 |
| gtfs_calendar_dates | 56 |

Bộ này có hiệu lực 27/07–05/09/2026: chỉ dùng thử import tại ngày kiểm thử.
Trước khi join realtime mới, lấy feed phù hợp. Thiết kế current-feed này chưa
đảm bảo đối chiếu lịch sử realtime qua nhiều phiên bản; đó là bước nâng cấp sau.
dbt runtime/models chưa được triển khai trong thay đổi này.
