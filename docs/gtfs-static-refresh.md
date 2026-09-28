# GTFS Static: quy trình cập nhật và báo cáo triển khai

GTFS Static là bộ tham chiếu cho realtime: `trip_id` giúp tìm chuyến và lịch dừng;
`route_id` giúp tìm tên tuyến; `stop_id` giúp tìm tên/vị trí điểm dừng. Đây là một
batch job độc lập, không đi qua Kafka hoặc Spark.

Thiết kế hiện tại giữ **một feed hiện hành trong PostgreSQL**, đồng thời giữ các
ZIP nguồn theo checksum trên đĩa. Refresh thay dữ liệu, không tạo schema hoặc
migration mới cho mỗi phiên bản feed.

## 1. Chạy như thế nào?

Chạy các lệnh từ thư mục gốc project. Chuẩn bị một lần nếu môi trường chưa có:

```bash
uv pip install --python .venv/bin/python -e '.[gtfs,dbt,dev]'
test -f dbt/profiles.yml || cp dbt/profiles.yml.example dbt/profiles.yml
docker compose up -d --wait postgres
```

`.env` phải có thông tin kết nối đúng: `POSTGRES_USER`, `POSTGRES_PASSWORD`,
`POSTGRES_DB`, và port máy host `POSTGRES_HOST_PORT`. Khi chạy trên Mac,
`POSTGRES_HOST` là `localhost`; khi chạy trong mạng Docker, dùng hostname service.
Loader ưu tiên `POSTGRES_PORT` nếu biến này tồn tại, vì vậy không để nó mâu thuẫn
với `POSTGRES_HOST_PORT` khi chạy local. Không commit mật khẩu hoặc `.env`.

Schema phải được tạo bằng migration 005; xem [hướng dẫn import](testing/gtfs-static-import.md)
nếu đây là database mới hoặc chưa có bảng. Với database đã chuẩn bị trong lần này,
không cần chạy lại migration mỗi lần refresh.

Lệnh dùng thường xuyên:

```bash
./scripts/refresh-gtfs.sh
```

Script tự xác định thư mục project, đọc `.env` đáng tin cậy, dùng Python/dbt trong
`.venv`, cập nhật dữ liệu rồi chạy `dbt build` cho staging và intermediate.
Có thể đổi executable qua `GTFS_PYTHON` và `GTFS_DBT` nếu sau này chạy trong container.

Một số biến thể:

```bash
# ZIP đã tải sẵn: không gọi mạng, vẫn kiểm tra và lưu archive trước khi nạp.
./scripts/refresh-gtfs.sh --zip-path /duong/dan/MBTA_GTFS.zip

# Nạp lại dù checksum không đổi, ví dụ sau khi dữ liệu bị sửa/xóa thủ công.
./scripts/refresh-gtfs.sh --force

# Tăng thời gian tải khi mạng chậm; giá trị mặc định là 900 giây.
./scripts/refresh-gtfs.sh --download-timeout 1200
```

Nguồn mặc định là `https://cdn.mbta.com/MBTA_GTFS.zip`; có thể cấu hình bằng
`MBTA_GTFS_STATIC_URL` hoặc `--url`. `--url` và `--zip-path` không dùng cùng nhau.

## 2. Flow hoàn chỉnh

```text
MBTA ZIP / ZIP local
  → tải/copy vào thư mục tạm
  → kiểm tra ZIP, ngày hiệu lực và header
  → lưu archive theo SHA-256
  → transaction PostgreSQL: so checksum → COPY 6 bảng → kiểm tra → ghi metadata
  → COMMIT
  → dbt build staging + intermediate + các data test liên quan
```

1. Tải theo từng khối, giới hạn ZIP 256 MiB, tổng thời gian tải mặc định 900 giây,
   timeout kết nối/đọc tối đa 30 giây và tối đa 3 lần thử với lỗi mạng/HTTP tạm thời.
   File tải dở không được coi là feed hoàn chỉnh.
2. Kiểm tra CRC của ZIP, giới hạn dung lượng khai báo sau giải nén 1 GB,
   `feed_info.txt`, sáu CSV bắt buộc và header. Ngày hiệu lực được so với ngày
   hiện tại ở `America/New_York`; feed hết hạn hoặc chưa có hiệu lực bị từ chối.
3. Tính SHA-256 từ toàn bộ byte ZIP và lưu tại
   `data/raw/gtfs_static/<sha256>/MBTA_GTFS.zip`. Không ghi đè bản archive khác;
   nếu cùng checksum đã tồn tại nhưng file bị hỏng, dừng và giữ nguyên file đó.
4. Mở transaction và lấy advisory lock theo schema: hai loader cùng schema không
   được thay dữ liệu đồng thời. Kiểm tra schema đích trước khi xóa bất kỳ dòng nào.
5. Nếu checksum đã nạp giống hệt, bỏ qua ghi database; `--force` bỏ qua tối ưu này.
   Cùng tên `feed_version` nhưng nội dung ZIP khác vẫn được nạp lại.
6. Dùng `DELETE` rồi PostgreSQL `COPY` cho sáu bảng trong cùng transaction.
   Kiểm tra kiểu dữ liệu, khóa, CHECK, bảng bắt buộc không rỗng và tham chiếu giữa
   trip/route, stop_time/trip/stop, stop/parent_station, trip/service.
7. Ghi `raw.gtfs_feed_state` rồi commit cùng sáu bảng. Bất kỳ lỗi nạp/validation
   nào trước commit đều rollback dữ liệu và metadata, giữ feed cũ.
8. Sau commit mới chạy dbt. Cả lần `loaded` lẫn `unchanged` đều chạy dbt, nên có
   thể chạy lại cùng lệnh sau khi sửa một model hoặc một lỗi dbt.

Hai khái niệm cần phân biệt: `feed_version` là nhãn phiên bản do MBTA cung cấp;
`sha256` là dấu vân tay của file dùng để quyết định nội dung có thay đổi hay chưa.
Khi checksum không đổi, script chỉ bỏ qua ghi PostgreSQL, không bỏ qua
tải/kiểm tra ZIP.

## 3. Đã làm ở những file nào?

| File | Trách nhiệm |
|---|---|
| `scripts/refresh_gtfs_static.py` | Tải/copy, kiểm tra, archive ZIP, gọi loader, xuất JSON kết quả và exit code. |
| `scripts/load_gtfs_static.py` | Kiểm tra feed/schema, khóa refresh, COPY sáu bảng, kiểm tra tham chiếu và cập nhật metadata trong một transaction. |
| `scripts/refresh-gtfs.sh` | Một entrypoint: nạp môi trường → refresh → dbt build/test; dừng khi một bước lỗi. |
| `database/migrations/005_create_gtfs_static_tables.sql` | Giữ định nghĩa sáu bảng nguồn; bổ sung bảng metadata riêng `raw.gtfs_feed_state`. |
| `dbt/models/intermediate/int_gtfs_trip_stop_schedule.sql` | Join theo ID của feed hiện hành; bỏ điều kiện join theo `feed_version` và bỏ hai cột đầu ra `feed_version`, `loaded_at` vốn không thuộc sáu CSV. |
| `tests/test_gtfs_static_loader.py` | Kiểm tra COPY, schema drift, rollback, checksum, ngày hiệu lực, cạnh tranh loader và ZIP thật. |
| `tests/test_gtfs_refresh.py` | Kiểm tra tải/retry/timeout, archive, chế độ ZIP local, skip/force, exit code và thứ tự chạy wrapper. |
| `.github/workflows/ci.yml` | Job GTFS riêng dùng PostgreSQL tạm và fixture để chạy integration tests, không cần tải feed thật từ mạng. |

`gtfs_feed_state` chỉ có một dòng cho feed đã commit thành công: phiên bản,
checksum, ngày hiệu lực, URL nguồn, đường dẫn ZIP, số dòng và thời điểm nạp.
Không nhân bản metadata vào hàng triệu dòng của `gtfs_stop_times`.
Chế độ ZIP local không tự đoán URL nguồn nên `source_url` là NULL khi thực sự nạp.

## 4. Kiểm tra sau mỗi lần chạy

Output JSON có `status: loaded` hoặc `status: unchanged`, phiên bản, checksum và
archive path. `row_counts` là NULL khi bỏ qua nạp; xem số dòng đã nạp trong state.
Sau đó dbt phải kết thúc không có lỗi. Mở PostgreSQL:

```bash
docker compose exec postgres sh -c 'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

```sql
SELECT feed_version, feed_start_date, feed_end_date, loaded_at,
       sha256, archive_path, row_counts
FROM raw.gtfs_feed_state;

SELECT count(*) FROM raw.gtfs_stop_times;
SELECT * FROM mart_intermediate.int_gtfs_trip_stop_schedule LIMIT 5;
```

Nếu chỉ muốn chạy lại dbt sau khi sửa model:

```bash
set -a
source .env
set +a
.venv/bin/dbt build --project-dir dbt --profiles-dir dbt \
  --select path:models/staging path:models/intermediate
```

## 5. Thay đổi database hiện có và kết quả kiểm chứng

Database trước lần này dùng sáu bảng legacy thiếu một số cột CSV và có cột
`feed_version NOT NULL` không có trong nguồn. Chạy lại `CREATE TABLE IF NOT EXISTS`
không sửa được cấu trúc đó. Đã chuẩn hóa cấu trúc **một lần** như sau:

1. Tạo schema ứng viên `gtfs_refresh_bootstrap_20260926` theo migration 005.
2. Nạp feed mới vào ứng viên, kiểm tra sáu bảng và metadata trước khi đổi `raw`.
3. Trong một transaction, chuyển sáu bảng cũ sang `raw_gtfs_archive_20260926`,
   rồi đưa sáu bảng mới cùng state vào `raw`.
4. Xóa schema ứng viên đã rỗng, không dùng CASCADE; chạy lại dbt để view trỏ vào
   bảng mới. Đã xác nhận không còn view nào tham chiếu bảng archive.

Sáu bảng cũ vẫn được giữ để khôi phục, không bị xóa. Bảng realtime không bị thay
đổi: vehicle 3.058 dòng, trip-stop 141.381 dòng, alert 5.990 dòng.
Không dùng `docker compose down -v`.

Feed thật đã tải và nạp ngày 2026-09-26:

- Phiên bản: `Fall 2026, 2026-09-24T17:51:42+00:00, version D`.
- Hiệu lực: 2026-09-17 đến 2026-12-12.
- SHA-256: `e3fdffe291bbe715d09356c1f2ceb58c8fc0781478e4041ba46160d964723133`.
- Archive: `data/raw/gtfs_static/<sha256>/MBTA_GTFS.zip` với checksum trên.
- Số dòng mới: routes 402; stops 10.279; trips 123.875; stop_times 3.174.678;
  calendar 155; calendar_dates 132.

Kết quả kiểm chứng:

- Kiểm tra lại ngày 2026-09-27 (giờ Việt Nam): 56 test pass trong 34,57 giây;
  wrapper trả `unchanged`, dbt vẫn `PASS=33`. Số dòng thực tế khớp metadata,
  sáu bảng archive còn nguyên và không có view trỏ vào archive.
- 56 tests GTFS pass, bao gồm test PostgreSQL thật và nạp ZIP thật hai lần;
  coverage hai script 90,70% (loader 96%, refresh 86%).
- dbt: 14 view và 19 data test thành công, tổng `PASS=33`, `ERROR=0`.
  dbt vẫn báo ba config path chưa dùng cho marts/seeds/snapshots; không phải
  lỗi dữ liệu hoặc test thất bại.
- Chạy wrapper lần nữa bằng cùng ZIP cho `unchanged`, không đổi `loaded_at`,
  nhưng vẫn build/test dbt thành công.
- Ruff, kiểm tra format, cú pháp shell và `git diff --check` đều pass.
- Workflow GitHub đã được bổ sung nhưng chưa chạy trên GitHub; chưa commit hoặc
  push trong lần triển khai này. CI bỏ qua test ZIP thật nếu không có `GTFS_TEST_ZIP`.

Lần kiểm thử với feed thật dùng ZIP MBTA đã tải và kiểm tra CRC, sau đó chạy
wrapper bằng `--zip-path`. Các tình huống HTTP retry/timeout được kiểm thử bằng
network mock; không coi đó là bằng chứng mọi điều kiện mạng thực tế đều đã được thử.
Chạy lại bài kiểm thử không cần tải mạng, sau khi nạp `.env`:

```bash
set -a
source .env
set +a
RUN_POSTGRES_INTEGRATION=1 .venv/bin/python -m pytest -o addopts='' -q \
  tests/test_gtfs_static_loader.py tests/test_gtfs_refresh.py \
  --cov=scripts.load_gtfs_static --cov=scripts.refresh_gtfs_static \
  --cov-report=term-missing --cov-fail-under=80
```

Kết quả cấu hình này: 55 pass, 1 skip (test ZIP thật), coverage 90,70%.
Muốn chạy đủ 56 test, truyền thêm `GTFS_TEST_ZIP` trỏ đến ZIP còn hiệu lực.

Theo quy trình TDD, các test cho kiểm tra ngày hiệu lực, metadata, checksum và
schema legacy được bổ sung trước phần logic tương ứng: lượt RED ban đầu có
6 test thất bại vì thiếu hành vi, sau triển khai chuyển sang GREEN. Các test
PostgreSQL dùng schema riêng, nên kiểm thử rollback không phá dữ liệu project.
Không tạo checkpoint commit trong lần này; thay đổi vẫn ở working tree để bạn review.

Điểm có ý nghĩa với project: số vehicle record khớp trip tăng từ 74 lên 2.880
trên 3.058 dòng; số trip-stop khớp lịch static tăng từ 1.031 lên 140.602 trên
141.381 dòng. Đây là bằng chứng feed cũ làm join thiếu, không phải cứ SQL chạy
thành công là dữ liệu tham chiếu đã phù hợp. Không đòi alert nào cũng có trip:
alert có thể chỉ áp dụng ở mức tuyến hoặc điểm dừng.

## 6. Giới hạn và hướng tự động hóa

- Transaction bảo vệ sáu bảng raw cùng metadata, **không bao gồm dbt**. Nếu dbt
  lỗi sau commit, raw đã là feed mới; sửa lỗi rồi chạy lại lệnh. Không báo toàn bộ
  flow thành công chỉ vì bước load thành công.
- Refresh thường không đổi cấu trúc bảng hoặc tạo migration. Nếu MBTA thêm cột
  chưa có trong database, loader dừng để tránh bỏ dữ liệu âm thầm; khi đó mới cần
  migration thay đổi schema. Giữ file 005 làm định nghĩa khởi tạo, không xóa nó.
- ZIP archive giữ lịch sử nguồn, nhưng sáu bảng raw chỉ giữ feed hiện hành. Join
  realtime lịch sử với static mới nhất vẫn có thể không khớp `trip_id`; đây chưa
  phải thiết kế historical/as-of join hoặc SCD2.
- Feed hết hạn bị chặn trong flow thường. Cờ `--allow-expired` chỉ có ở loader
  cấp thấp để kiểm thử/import lịch sử có chủ đích, không nên dùng cho refresh định kỳ.
- Advisory lock chỉ phối hợp các loader này, không khóa mọi ứng dụng ghi raw.
  Lock chỉ bao quanh bước nạp database, không serialize các lần dbt chạy phía sau.
  `DELETE`/`COPY` sinh WAL và dead tuples; PostgreSQL cần autovacuum hoạt động bình thường.
- Chưa tự động xóa archive ZIP cũ hoặc schema archive legacy. Chỉ dọn sau khi đã
  xác nhận không cần khôi phục; dữ liệu trong `data/` không được commit lên Git.
- Sau này Airflow có thể gọi lại entrypoint này trên môi trường có Python, dbt,
  mạng/volume và secrets phù hợp; dùng `max_active_runs=1`, retry hữu hạn và cảnh
  báo khi exit code khác 0. `max_active_runs=1` cũng tránh hai lần dbt chạy chồng
  nhau. Chưa tạo lịch/DAG tự động trong phạm vi lần này.
