# Nạp GTFS Static trực tiếp bằng PostgreSQL COPY

Đây là hướng dẫn loader cấp thấp. Để tải feed mới, lưu archive và chạy dbt bằng
một lệnh, dùng [quy trình refresh GTFS Static](../gtfs-static-refresh.md):

```bash
./scripts/refresh-gtfs.sh
```

## Phạm vi và schema

Loader đọc sáu CSV tại gốc ZIP MBTA: `routes.txt`, `stops.txt`, `trips.txt`,
`stop_times.txt`, `calendar.txt`, `calendar_dates.txt`. `feed_info.txt` dùng kiểm
tra phiên bản/ngày hiệu lực. Các file MBTA khác chưa thuộc phạm vi import.

| Bảng | Khóa chính |
|---|---|
| raw.gtfs_routes | route_id |
| raw.gtfs_stops | stop_id |
| raw.gtfs_trips | trip_id |
| raw.gtfs_stop_times | trip_id, stop_sequence |
| raw.gtfs_calendar | service_id |
| raw.gtfs_calendar_dates | service_id, date |

Sáu bảng giữ cột nguồn, bao gồm extension MBTA; không thêm `feed_version` hoặc
`loaded_at` vào từng dòng. Metadata nằm riêng trong một dòng `raw.gtfs_feed_state`.
ID dùng TEXT để giữ số 0 đầu; giờ dùng TEXT để giữ `25:10:00`; ngày dùng DATE.
COPY giữ quy ước CSV PostgreSQL: ô rỗng không quote thành NULL, `""` thành chuỗi rỗng.

## Chuẩn bị database

Database mới chạy migration khi PostgreSQL khởi tạo volume. Với database đã có
schema `raw` nhưng chưa có bảng GTFS, chạy từ thư mục gốc project:

```bash
docker compose up -d --wait postgres
docker compose exec -T postgres sh -c \
  'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
  < database/migrations/005_create_gtfs_static_tables.sql
```

Chạy lại 005 trên cấu trúc đúng không xóa dữ liệu và bổ sung `gtfs_feed_state`
nếu chưa có. `CREATE TABLE IF NOT EXISTS` **không sửa bảng legacy có cấu trúc sai**;
cần migration/chuẩn hóa có kế hoạch, không xóa schema hoặc volume để thử lại.

## Import một ZIP đã có

Local cần các dependency `.[gtfs,dev]`. Loader này không tự tải, không tự archive
và không chạy dbt; nó luôn nạp lại ZIP được truyền vào. Chọn file còn hiệu lực:

```bash
set -a
source .env
set +a
.venv/bin/python -m scripts.load_gtfs_static /duong/dan/MBTA_GTFS.zip
```

`POSTGRES_HOST`/port phải trỏ đúng database từ máy đang chạy Python.
Feed hết hạn hoặc chưa có hiệu lực bị từ chối mặc định. Chỉ khi chủ đích kiểm thử
dữ liệu lịch sử, thêm `--allow-expired`; không dùng cờ này trong refresh hiện hành.

## Bảo đảm khi nạp

1. Kiểm tra CRC, giới hạn ZIP giải nén 1 GB, metadata/ngày và header sáu CSV.
2. Mở transaction, lấy advisory lock theo schema; kiểm tra cột CSV có đủ ở bảng
   đích và bảng đích không yêu cầu cột bắt buộc không có trong CSV.
3. DELETE rồi COPY từng bảng theo khối 65.536 ký tự; thứ tự cột lấy từ header.
   Không giải nén file ra đĩa hoặc INSERT từng dòng. Cột optional không có trong
   CSV được để mặc định của bảng.
4. Kiểm tra kiểu/PK/NOT NULL/CHECK; routes, stops, trips, stop_times không được
   rỗng; calendar và calendar_dates có thể rỗng. Kiểm tra các tham chiếu nguồn.
5. Ghi checksum, phiên bản, ngày, đường dẫn ZIP và row counts vào state rồi COMMIT.
   Bất kỳ lỗi nào trước commit đều rollback sáu bảng **và metadata**.

Refresh thay toàn bộ snapshot hiện hành, không cộng dồn. DELETE giữ identity của
bảng. Reader thông thường vẫn đọc dữ liệu đã commit trước đó trong khi load;
nếu nhiều query phải thấy cùng một snapshot, dùng transaction REPEATABLE READ.
Advisory lock không ngăn chương trình khác bỏ qua lock rồi ghi trực tiếp vào raw.

## Kiểm tra và chạy tests

```sql
SELECT feed_version, feed_start_date, feed_end_date, loaded_at, row_counts
FROM raw.gtfs_feed_state;
SELECT count(*) FROM raw.gtfs_stop_times;
```

Tests PostgreSQL tạo/xóa schema riêng `test_gtfs_<uuid>`, không thay raw hiện hành.
Nạp `.env` như trên rồi chạy:

```bash
RUN_POSTGRES_INTEGRATION=1 .venv/bin/python -m pytest -o addopts='' \
  --cov=scripts.load_gtfs_static --cov=scripts.refresh_gtfs_static \
  --cov-report=term-missing --cov-fail-under=80 -q \
  tests/test_gtfs_static_loader.py tests/test_gtfs_refresh.py
```

Để kiểm thử ZIP thật, thêm `GTFS_TEST_ZIP=/duong/dan/MBTA_GTFS.zip` trước lệnh.
Dùng feed còn hiệu lực; test này đối chiếu header/số dòng nguồn và nạp hai lần.
`-o addopts=''` tránh đưa coverage mặc định của Spark/producer vào bài test GTFS.

Phạm vi kiểm thử gồm rollback, replay không nhân đôi, loại bỏ dòng cũ, schema
drift, metadata/checksum, hai loader cạnh tranh, CSV quoting/BOM/UTF-8,
retry/timeout qua network mock và wrapper chỉ chạy dbt khi refresh thành công.
Kết quả thực thi mới nhất được ghi trong [báo cáo refresh](../gtfs-static-refresh.md).
Chưa có fault-injection kill process thực tế giữa một transaction đang chạy.
