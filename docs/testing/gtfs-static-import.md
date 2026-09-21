# Nạp GTFS Static trực tiếp bằng PostgreSQL COPY

## Phạm vi và schema

Loader nạp sáu CSV ở gốc ZIP MBTA: `routes.txt`, `stops.txt`, `trips.txt`,
`stop_times.txt`, `calendar.txt`, `calendar_dates.txt`. `feed_info.txt` được đọc
để kiểm tra metadata và cảnh báo hết hạn, không được thêm vào từng dòng dữ liệu.
Các file MBTA khác trong ZIP chưa thuộc phạm vi loader này.

Sáu bảng `raw.gtfs_*` chứa đầy đủ cột của sáu CSV trong ZIP tham chiếu
`2026-08-03_version-D`, bao gồm các extension MBTA. Không thêm `feed_version`
hay `loaded_at` vào bảng. Giữ ZIP gốc để tra phiên bản và lịch sử nguồn.
ID dùng TEXT để giữ số 0 đầu; giờ dùng TEXT để giữ `25:10:00`; ngày dùng DATE.
COPY giữ quy ước CSV PostgreSQL: ô rỗng không quote thành NULL, `""` thành
chuỗi rỗng. Không tự sửa giá trị nguồn trước khi nạp.

| Bảng | Khóa chính |
|---|---|
| raw.gtfs_routes | route_id |
| raw.gtfs_stops | stop_id |
| raw.gtfs_trips | trip_id |
| raw.gtfs_stop_times | trip_id, stop_sequence |
| raw.gtfs_calendar | service_id |
| raw.gtfs_calendar_dates | service_id, date |

## Flow

1. Kiểm tra ZIP (tối đa 1 GB giải nén), metadata và header cả sáu CSV.
2. Mở một transaction, lấy advisory lock để serialize các loader cùng schema.
3. Kiểm tra mọi cột CSV đều tồn tại trong bảng đích. Header trùng, thiếu cột
   bắt buộc, cột nguồn mới chưa có trong database đều bị từ chối, không bỏ qua.
4. Với từng bảng: DELETE dữ liệu cũ rồi COPY trực tiếp từ ZIP theo từng khối
   65.536 ký tự. Không tạo bảng tạm, không INSERT từng dòng, không giải nén ra đĩa.
   Danh sách cột COPY lấy theo thứ tự header, nên CSV đổi thứ tự cột vẫn đúng.
   Cột optional không có trong CSV được để mặc định của bảng (NULL).
5. PostgreSQL kiểm tra kiểu dữ liệu, PK, NOT NULL và CHECK trong lúc COPY.
   Bốn bảng routes/stops/trips/stop_times không được rỗng; hai calendar có thể rỗng.
6. Kiểm tra trip → route, stop_time → trip/stop, stop → parent_station và
   trip → service trong calendar hoặc calendar_dates có exception_type=1.
7. COMMIT một lần. Bất kỳ lỗi COPY hoặc validation nào cũng rollback cả sáu bảng.

Chạy lại thay toàn bộ feed hiện hành, không cộng dồn. DELETE bảo toàn identity
của bảng. Reader thông thường vẫn đọc dữ liệu cũ trước commit; để nhiều query
cùng thấy một snapshot, dùng transaction REPEATABLE READ. Refresh sinh WAL/dead
tuples và giữ write lock trong lúc nạp, phù hợp refresh định kỳ. Advisory lock
phối hợp các loader này, không ngăn mọi chương trình bên ngoài ghi vào raw.

## Chạy từ thư mục project

```bash
test -f .env || cp .env.example .env
```

Với lần cài đặt đầu, đặt mật khẩu local trong `.env` trước khi khởi tạo database.

```bash
docker compose up -d --wait postgres
docker compose ps postgres
```

Chờ PostgreSQL healthy. `.env` cung cấp thông tin kết nối cho Compose.
Nếu chưa có image local chứa psycopg:

```bash
docker compose build spark-master
```

GTFS chỉ có một migration: `005_create_gtfs_static_tables.sql`, định nghĩa đầy đủ
sáu bảng theo CSV nguồn. Database mới tự chạy file này khi PostgreSQL khởi tạo.
Nếu database đã khởi tạo nhưng chưa có các bảng GTFS, chạy:

```bash
docker compose exec -T postgres sh -c \
  'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
  < database/migrations/005_create_gtfs_static_tables.sql
```

Chạy lại 005 trên schema đúng không xóa dữ liệu. Loader chỉ nạp dữ liệu, không
tự tạo hoặc thay cấu trúc bảng. `CREATE TABLE IF NOT EXISTS` không sửa cấu trúc
của một bảng đã tồn tại nhưng không khớp định nghĩa.

ZIP nguồn không đi kèm khi clone repo. Tải một bản MBTA vào thư mục riêng
(hoặc dùng ZIP đã có và thay đường dẫn trong lệnh import):

```bash
mkdir -p data/raw/gtfs_static/downloaded
curl --fail --location https://cdn.mbta.com/MBTA_GTFS.zip \
  --output data/raw/gtfs_static/downloaded/MBTA_GTFS.zip
unzip -t data/raw/gtfs_static/downloaded/MBTA_GTFS.zip
unzip -p data/raw/gtfs_static/downloaded/MBTA_GTFS.zip feed_info.txt
```

Nạp ZIP:

```bash
docker compose run --rm --no-deps --entrypoint python3 \
  -v "$PWD/scripts:/opt/transitpulse/scripts:ro" \
  -v "$PWD/data:/opt/transitpulse/data:ro" \
  spark-master \
  /opt/transitpulse/scripts/load_gtfs_static.py \
  /opt/transitpulse/data/raw/gtfs_static/downloaded/MBTA_GTFS.zip
```

Lệnh chỉ chạy Python trong container dùng một lần; không khởi tạo Spark, Kafka
hay dbt. Với ZIP mới, thay đường dẫn cuối. Feed mẫu hết hạn ngày 05/09/2026:
vẫn import được để kiểm thử, nhưng cần feed phù hợp khi join realtime hiện tại.

## Kiểm tra kết quả

```bash
docker compose exec postgres sh -c 'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

```sql
\dt raw.gtfs_*
SELECT route_id, route_desc, route_fare_class, network_id FROM raw.gtfs_routes LIMIT 5;
SELECT trip_id, route_pattern_id FROM raw.gtfs_trips LIMIT 5;
SELECT count(*) FROM raw.gtfs_stop_times;
```

Xem metadata trong ZIP thay vì query cột tự thêm:

```bash
unzip -p data/raw/gtfs_static/downloaded/MBTA_GTFS.zip feed_info.txt
```

## Kiểm thử — 2026-09-21

Tests tạo/xóa schema riêng `test_gtfs_<uuid>`, không thay dữ liệu raw hiện hành.
32 test GTFS pass trên PostgreSQL thật; coverage loader 96%. Test ZIP thật
đối chiếu toàn bộ header với schema, đếm bản ghi nguồn rồi
kiểm tra số dòng sau hai lần import (2.237.146 stop_times mỗi lần).

Bao gồm reload không nhân đôi và loại bỏ dòng cũ; rollback cả sáu bảng sau lỗi
CSV/kiểu dữ liệu/CHECK/PK/tham chiếu; BOM, header đảo thứ tự, dấu phẩy, newline
trong quoted field, UTF-8, NULL/chuỗi rỗng; giữ các cột extension; chạy lại 005
giữ nguyên dữ liệu và vẫn import tiếp được.

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

Để chạy thêm test ZIP thật, thêm hai option trước `--workdir`:

```bash
  -v "$PWD/data:/opt/transitpulse/data:ro" \
  -e GTFS_TEST_ZIP=/opt/transitpulse/data/raw/gtfs_static/2026-08-03_version-D/MBTA_GTFS.zip \
```

Chưa kiểm thử lỗi mạng/process kill giữa transaction hoặc nhiều loader cạnh tranh.
