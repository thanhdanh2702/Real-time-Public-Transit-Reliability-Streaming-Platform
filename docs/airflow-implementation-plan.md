# TransitPulse — xây dựng lại Airflow

Ngày: 2026-10-10.

## 1. Trạng thái khởi đầu sau rollback

Đã revert riêng merge PR #18, đưa phần Airflow về trạng thái ở commit
`b5f047c`. Không reset lịch sử Git và không rollback dashboard.

Đã gỡ khỏi phiên bản hiện tại:

- DAG `airflow/dags/refresh_dashboard_marts.py`.
- Helper chạy dbt trong `airflow/include/transitpulse_airflow/`.
- Profile dbt riêng, Dockerfile, requirements và test của triển khai Airflow đó.
- Cấu hình build image, môi trường và service DAG processor được thêm trong PR #18.
- Tài liệu kiểm chứng `docs/testing/dependency-security-upgrade.tdd.md` của PR đó.

Vẫn giữ nguyên:

- Producer, Kafka, ba Spark job, PostgreSQL, dbt và dashboard Recent/Historical.
- Các chỉnh sửa local chưa commit của dashboard và tài liệu khác.
- Khung Compose: `airflow-init`, `airflow-api-server`, `airflow-scheduler`,
  profile `orchestration`, database metadata `airflow` và volume mount.
- Dữ liệu PostgreSQL, checkpoint, image Docker đã build và file `.env` local.

**Khung Compose cũ chưa được xác nhận chạy hoàn chỉnh với Airflow 3.** Nó còn
cấu hình secret mặc định của giai đoạn đầu; cần sửa trước khi sử dụng. Rollback
source code không tự gỡ DAG/run history cũ khỏi database metadata hoặc thay đổi
container đang tồn tại. Không cần xóa database để bắt đầu viết lại.

Tài liệu `docs/airflow-roles-and-status.md` là ghi nhận trước rollback, không phải
checklist hoàn thành hiện tại. Dùng kế hoạch này làm mốc mới.

## 2. Airflow phụ trách gì?

Luồng streaming liên tục vẫn do Docker Compose quản lý:

`MBTA realtime → producer → Kafka → ba Spark job → staging PostgreSQL`.

Airflow quản lý các công việc hữu hạn theo lịch:

1. Chạy dbt để cập nhật các model phục vụ dashboard.
2. Cập nhật GTFS tĩnh và rebuild các model phụ thuộc khi feed thay đổi.
3. Theo dõi trạng thái, thời gian chạy, retry, lỗi và kiểm tra dữ liệu phù hợp.

Không đưa Spark streaming vào task Airflow chạy vô hạn. Airflow cũng không phải
là cơ chế tự refresh giao diện dashboard.

## 3. Thứ tự triển khai

### Bước 1 — dựng môi trường Airflow

- [ ] Bắt đầu nhánh làm việc từ `main` sau rollback. Nhánh `feature/airflow` cũ
  vẫn chứa triển khai trước đó; không checkout lại nhánh đó rồi coi là mốc mới.
- [ ] Viết `airflow/Dockerfile` và `airflow/requirements.txt`: cài dbt-postgres
  và các dependency thực sự cần, kiểm tra tương thích với image Airflow.
- [ ] Cập nhật `docker-compose.yml`: API server, scheduler, DAG processor,
  database migration và healthcheck phù hợp với phiên bản Airflow đang dùng.
- [ ] Đưa secret vào biến môi trường; không dùng secret hardcode/mặc định cũ.
- [ ] Kiểm tra quyền ghi logs/artifacts và quyền đọc dbt/scripts/data.
- [ ] Tạo profile dbt cho container: host `postgres`, port nội bộ `5432`.
  Không dùng port host của PostgreSQL trên Mac cho kết nối giữa container.

Đạt khi: Compose parse được, Airflow UI truy cập được, không có import error,
và `dbt debug` chạy thành công **trong môi trường task Airflow**.

### Bước 2 — viết helper chạy dbt

Vị trí dự kiến: `airflow/include/transitpulse_airflow/dbt_refresh.py`.

- [ ] Gọi `dbt build` bằng danh sách đối số rõ ràng, không ghép shell command
  từ đầu vào tùy ý.
- [ ] Có timeout; subprocess thất bại phải làm task thất bại, không nuốt lỗi.
- [ ] Log đủ để tìm model/test lỗi, không log password hoặc connection URL.
- [ ] Ghi trạng thái vào `ops.dbt_run_history` theo schema hiện có; retry
  không tạo nhiều bản ghi cho cùng một DAG run.
- [ ] Phân biệt task failure với lỗi ghi metadata, tránh che mất lỗi gốc.
- [ ] Tách thư mục target/log theo lần chạy để không đè artifact của nhau.

Đạt khi: test được success, lỗi dbt và timeout; dashboard đọc được trạng thái
run mới. Có bảng metadata sẵn không đồng nghĩa Airflow đang cập nhật nó.

### Bước 3 — tạo DAG refresh dashboard

Vị trí dự kiến: `airflow/dags/refresh_dashboard_marts.py`.

- [ ] Lịch khởi đầu: mỗi 5 phút, `catchup=False`, `max_active_runs=1`.
- [ ] Chạy dbt build với các model dashboard cần và các dependency của chúng.
  Đối chiếu query dashboard để không bỏ sót dimension hoặc mart Historical.
- [ ] Có retry hữu hạn, timeout task và timeout DAG; tránh lần chạy kéo dài
  tạo hàng đợi vô hạn.
- [ ] Dùng `dbt build` để chạy cả model và test được chọn.
- [ ] Kiểm tra kết quả sau chạy: model tồn tại, dữ liệu/thời gian cập nhật hợp lý.

Đạt khi: trigger thủ công một DAG run thành công, dashboard thấy dữ liệu từ
mart mới, và lịch tự chạy được ít nhất hai lần. Lưu ý dbt build nhiều model
không mặc nhiên publish tất cả model trong một transaction duy nhất.

### Bước 4 — ngăn các lần cập nhật đụng nhau

Phải hoàn thành trước khi thêm DAG GTFS:

- [ ] Cho task refresh mart và task GTFS-refresh-plus-build dùng chung một
  Airflow pool có một slot, giữ slot suốt quá trình import/build.
- [ ] Không chạy dbt thủ công đồng thời nếu chưa có cơ chế khóa dùng chung.
- [ ] Nếu cần hỗ trợ CLI song song, bổ sung cùng một advisory-lock protocol
  cho cả CLI và Airflow, không chỉ khóa ở một bên.

`max_active_runs=1` chỉ hạn chế từng DAG riêng; không chặn hai DAG khác nhau.
Khóa hiện có trong loader GTFS chỉ bảo vệ import GTFS, không bảo vệ toàn bộ
dbt build. Airflow pool không kiểm soát lệnh dbt người dùng tự chạy ngoài Airflow.

### Bước 5 — tự động cập nhật GTFS tĩnh

Vị trí dự kiến: `airflow/dags/refresh_gtfs_static.py`.

- [ ] Tái sử dụng `scripts/refresh_gtfs_static.py` và loader hiện có; không
  viết lại download/import hoặc tạo migration mới mỗi lần feed đổi.
- [ ] Mount/copy đủ scripts và thư mục lưu ZIP mà container cần.
- [ ] Lịch khởi đầu: mỗi ngày, chọn giờ và timezone rõ ràng.
- [ ] Download → validate ZIP → so checksum → import khi thay đổi → dbt build
  các model phụ thuộc → kiểm tra kết quả.
- [ ] Feed không đổi thì bỏ qua import; trả summary nhỏ qua XCom, không đưa
  ZIP/DataFrame/password vào XCom.
- [ ] ZIP lỗi không được thay dữ liệu đang dùng. Import sáu bảng phải giữ
  tính nguyên tử của loader hiện có.
- [ ] Sau import thành công nhưng dbt thất bại, retry vẫn phải chạy lại phần
  build; không bỏ qua toàn bộ flow chỉ vì checksum lúc retry đã giống nhau.

`scripts/refresh-gtfs.sh` hiện build staging/intermediate; khi orchestration
cần phục vụ dashboard phải chọn thêm những model downstream cần thiết.

Đạt khi: feed hợp lệ cập nhật được, feed không đổi không import lại, feed lỗi
giữ nguyên dữ liệu đang dùng, và mart sau cập nhật vẫn qua test.

### Bước 6 — kiểm tra chất lượng và vận hành

- [ ] Xác định ngưỡng freshness riêng cho từng nguồn. Alert có thể không đổi
  trong nhiều lần polling; không có event mới chưa đủ để kết luận pipeline lỗi.
- [ ] Kiểm tra thiếu bảng/cột, freshness và kết quả dbt test; không chỉ đếm dòng.
- [ ] Viết test ngắn gọn cho helper, retry/timeout, profile, DAG import thật,
  và việc không chạy chồng giữa các DAG.
- [ ] Thêm CI kiểm tra image Airflow, dependency và DAG import. Test đọc
  source/so chuỗi không thay thế được kiểm thử runtime.
- [ ] Tài liệu hóa cách khởi động, trigger, xem logs, rerun và dừng services.

Thông báo ra Slack/email, remote logs, executor phân tán và backfill lịch sử
là phần nâng cao, chưa bắt buộc cho bản local đầu tiên.

## 4. Cấu trúc dự kiến sau khi xây dựng lại

```text
airflow/
├── Dockerfile
├── requirements.txt
├── dags/
│   ├── refresh_dashboard_marts.py
│   └── refresh_gtfs_static.py
├── include/
│   ├── dbt_profiles/profiles.yml
│   └── transitpulse_airflow/
│       ├── __init__.py
│       ├── dbt_refresh.py
│       └── gtfs_refresh.py
├── logs/
└── tests/
    ├── test_dbt_refresh.py
    ├── test_gtfs_refresh.py
    └── test_dags.py
```

Đây là kế hoạch, chưa tạo lại implementation. Chỉ thêm helper GTFS nếu cần
để nối orchestration với script hiện có; tránh thêm tầng trừu tượng không cần thiết.

## 5. Điều kiện hoàn thành

- [ ] Ba Spark job tiếp tục đưa dữ liệu vào staging mà không bị Airflow quản lý
  như những task vô hạn.
- [ ] Airflow tự cập nhật dbt marts đúng lịch và dashboard đọc được kết quả.
- [ ] GTFS refresh tái sử dụng loader hiện có và không chạy chồng với dbt refresh.
- [ ] Failure/retry/timeout được kiểm chứng, metadata và logs đủ để chẩn đoán.
- [ ] Secret không xuất hiện trong Git hoặc logs.
- [ ] Test, lint, Compose validation, DAG import và kiểm thử E2E đều đạt.

**Việc làm tiếp theo:** Bước 1 — hoàn thiện môi trường Airflow trước khi viết DAG.
