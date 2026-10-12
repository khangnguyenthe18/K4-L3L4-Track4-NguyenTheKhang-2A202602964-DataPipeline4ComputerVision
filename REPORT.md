# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Nguyễn Thế Khang` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602964` |
| GitHub | `khangnguyenthe18` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-05905` |

## Kết quả cuối

- `dataset_version` của bạn: `6cd8787625c0`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `train: 713, val: 146, test: 160 (tổng: 1019 khung)`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Dựa trên dữ liệu quan sát từ hai bảng `lake.bronze_raw_telemetry_a` (3021 dòng) và `lake.bronze_raw_telemetry_b` (3234 dòng), có 5 khác biệt lớn:
1. **Tên cột (Schema drift)**: Nguồn A đặt tên cột chuẩn `ts_utc`, `speed_mps`, `heading_deg`, `lat`, `lon`, `alt_m`, `gas`, `brake`, `steer`. Nguồn B dùng tên khác hoàn toàn: `timestamp_ms`, `spd_kmh`, `hdg`, `latitude`, `longitude`, `altitude`, `throttle_pct`, `brake_pct`, `steering`.
2. **Đơn vị đo vận tốc**: Nguồn A đo bằng mét/giây (`speed_mps`, ví dụ 1.213 m/s), nguồn B đo bằng kilômét/giờ (`spd_kmh`, ví dụ 55.11 km/h).
3. **Định dạng thời gian**: Nguồn A dùng chuẩn chuỗi ISO-8601 UTC (`2025-10-07T08:00:00.014Z`), còn nguồn B dùng số nguyên Unix Epoch mili-giây (`1759933801509`).
4. **Độ lệch đồng hồ (Clock Offset)**: Truy vấn `lake.bronze_video_files` cho thấy thiết bị đo ở drive B bị lệch đồng hồ 1500 ms (1.5 giây) so với mốc video, trong khi drive A có độ lệch bằng 0 ms.
5. **Thang đo chân ga/phanh**: Nguồn A đo theo dải số thập phân 0.0 - 1.0 (ví dụ `gas = 0.028`), còn nguồn B đo theo thang phần trăm 0 - 100% (`throttle_pct = 2.0`).

Pipeline giữ mỗi nguồn một bảng bronze riêng và để toàn bộ cột ở dạng chữ (string) vì:
- **Nguyên lý Medallion (Slide 14)**: Lớp Bronze lưu trữ dữ liệu thô nguyên trạng (raw as-is, append-only). Việc lưu kiểu chữ giúp quá trình nạp (ingestion) không bao giờ bị gián đoạn hay crash khi gặp ô trống (null), giá trị dị biệt hoặc lỗi cú pháp.
- **Bảo toàn dữ liệu & Dễ Replay**: Ép kiểu hoặc gộp ngay từ đầu dễ dẫn đến mất mát dữ liệu (data loss). Giữ nguyên trạng giúp đội ngũ kỹ sư có thể thay đổi luật làm sạch ở Silver và chạy lại (replay/audit) bất cứ khi nào. Đồng thời, video lớn được lưu riêng trên MinIO (`bronze/video/`), Delta Lake chỉ lưu con trỏ và mã sha256, giúp bảng nhẹ và truy vấn nhanh.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline đưa hai nguồn về một schema chuẩn thông qua các model staging của dbt:
- **Đơn vị**: Tốc độ nguồn B được đổi từ km/h sang m/s (`spd_kmh / 3.6`). Góc lái và bàn đạp được quy đổi về cùng thang đo chuẩn.
- **Kiểu thời gian & Độ lệch đồng hồ**: Chuyển đổi timestamp Unix epoch ms của nguồn B sang timestamp UTC, đồng thời tự động trừ đi 1500 ms độ lệch đồng hồ (`to_timestamp((timestamp_ms - 1500) / 1000.0)`).
- **Bằng chứng đúng**: Thời điểm bắt đầu của telemetry B (`min(ts)`) sau chuẩn hóa là `2025-10-08 14:30:00.009000+00:00`, trùng khớp chính xác với mốc bắt đầu của video B (`2025-10-08 14:30:00`) trong phạm vi 0.5s (test `drive B clock offset removed` báo PASS).
- **Bảng Quarantine thực tế** (tổng cộng 106 dòng bị cách ly):
  | drive_id | reject_reason | so_dong |
  |---|---|---|
  | drive_a | duplicate | 20 |
  | drive_a | speed_missing | 30 |
  | drive_a | speed_out_of_range | 3 |
  | drive_b | duplicate | 20 |
  | drive_b | speed_missing | 30 |
  | drive_b | speed_out_of_range | 3 |
- **Vì sao dòng lỗi vào `rejects` thay vì bị xóa**: Áp dụng mô hình Data Quality Quarantine. Dòng lỗi không bị xóa âm thầm nhằm bảo tồn dữ liệu phục vụ điều tra lỗi phần cứng cảm biến, theo dõi tỷ lệ suy giảm chất lượng dữ liệu (data degradation), và cho phép hồi phục dữ liệu khi luật làm sạch được cập nhật.

## Bước 3 · Silver frames

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

- **Phân biệt `warn` và `error`**:
  - `max_gap_ms` đặt mức **WARN**: Mất tín hiệu cảm biến trong thời gian ngắn là hiện tượng vật lý bình thường khi xe di chuyển qua đường hầm, tán cây hoặc khu vực sóng GPS yếu. Đây là chỉ báo vận hành (operational indicator) để theo dõi chất lượng, không nên dừng (break) toàn bộ pipeline.
  - `in_range` đặt mức **ERROR**: Vận tốc vượt ngưỡng vật lý (0..70 m/s tương đương 0..252 km/h) là dữ liệu rác nghiêm trọng. Nếu dữ liệu này lọt vào tập huấn luyện mô hình xe tự lái sẽ gây sai lệch hành vi điều khiển và đe dọa an toàn tính mạng, do đó bắt buộc phải chặn đứng pipeline ngay lập tức.
- **Kết quả thí nghiệm `break-gate` (`break_gate.json`)**:
  - Khi cố tình nới lỏng luật lọc tốc độ lên 1000 m/s (`relaxed_max_speed_mps: 1000`), dbt test cho kết quả: `PASS=5 WARN=1 ERROR=1 SKIP=0`.
  - Cổng chất lượng đã bắt đúng lỗi: test `in_range_silver_telemetry_speed_mps__70__0` báo FAIL và bắt được đúng **6 dòng** tốc độ vô lý (3 dòng của drive A và 3 dòng của drive B). Khi khôi phục luật gốc, test trở lại `PASS=6 WARN=1 ERROR=0`.
  - **Ý nghĩa**: Cổng chất lượng (Quality Gate) đóng vai trò lá chắn độc lập (fail-fast mechanism), đảm bảo dữ liệu vi phạm hợp đồng dữ liệu (data contract) không bao giờ bị rò rỉ sang các tầng phân tích Gold hoặc mô hình AI.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Dựa vào file bằng chứng `idempotency.json` chạy trên partition `trip-05905`:
- **Trước khi chạy lại**: Bảng `silver.frames` có **1246 dòng**, dấu vân tay SHA256 là `6c7480dfde7e`.
- **Sau 2 lần chạy lại liên tiếp**: Số dòng vẫn là **1246 dòng**, dấu vân tay vẫn là `6c7480dfde7e` (giá trị `unchanged: true`).
- **Khi mô phỏng ghi kiểu `append` sai**: Một trip có 60 khung sau 2 lần chạy bị đội lên **120 dòng** (bị nhân đôi 2x).

**Vì sao Delta `overwrite` theo partition an toàn hơn "xóa rồi ghi lại":**
- Delta Lake sử dụng cơ chế ghi đè phân vùng nguyên tử (`replaceWhere="trip_id = '...'"` kết hợp ACID Transaction log). Thao tác ghi chỉ tạo ra các file Parquet mới và commit vào transaction log ở trạng thái sẵn sàng. Nếu tiến trình bị sập nguồn hoặc crash giữa chừng, transaction bị hủy bỏ hoàn toàn (rollback), các reader vẫn đọc được bản ghi cũ nguyên vẹn.
- Ngược lại, phương pháp thủ công "xóa rồi ghi lại" gồm hai thao tác riêng biệt không có tính nguyên tử. Nếu hệ thống sập ngay sau lệnh xóa (DELETE/DROP) và trước khi kịp ghi dữ liệu mới, toàn bộ dữ liệu của phân vùng đó sẽ bị mất trắng (data corruption/loss).

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

- **Khung đen và Khung tối**:
  - *Khung đen* (`is_black`): Do camera bị lỗi cảm biến, đứt kết nối hoặc ống kính bị che kín hoàn toàn (`brightness < 5`). Không chứa bất kỳ thông tin thị giác nào $\rightarrow$ pipeline loại bỏ hoàn toàn khỏi tập huấn luyện Gold.
  - *Khung tối* (`is_dark`): Do xe di chuyển trong hầm, ban đêm hoặc dưới bóng cây râm mát (`brightness < 30`). Khung hình vẫn chứa đường nét xe, làn đường và biển báo $\rightarrow$ pipeline **gắn cờ nhưng giữ lại** (`dark_kept` ở drive B lên tới 61 khung).
  - *Quan điểm*: Tôi hoàn toàn đồng ý, vì mô hình thị giác máy tính của xe tự lái cần học dữ liệu thiếu sáng để vận hành ban đêm, trong khi khung đen hoàn toàn chỉ là nhiễu vô giá trị.
- **Bảng số khung trùng theo ngưỡng dHash từ `threshold.json`**:

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

- **Lựa chọn ngưỡng**: Tôi chọn ngưỡng **2** (ngưỡng mặc định của pipeline). Vì ngưỡng 0 quá khắt khe, chỉ bắt trùng bit tuyệt đối và bỏ sót các khung xe đứng yên có nhiễu cảm biến nhẹ. Ngưỡng 6 lại quá lỏng lẻo, loại bỏ nhầm tới 363 khung ở A và 244 khung ở B (mất các tình huống xe bò chậm ở giao lộ). Ngưỡng 2 cân bằng tối ưu giữa việc lọc khung đứng hình và giữ lại dữ liệu chuyển động.
- **Mối liên hệ với tốc độ xe**: Xe chạy nhanh (vận tốc lớn) thì phối cảnh thay đổi liên tục giữa các frame cách nhau 0.5s, khoảng cách dHash lớn nên số khung trùng rất ít (như drive B chỉ có 53 khung trùng). Xe chạy chậm hoặc dừng chờ đèn đỏ thì phối cảnh không đổi, số khung trùng tăng đột biến (như drive A có camera đứng hình 6 giây và xe chạy chậm có tới 149 khung trùng).

## Bước 4 · Gold

### Câu 6

(trả lời ở đây)

### Câu 7

(trả lời ở đây)

### Câu 8

(trả lời ở đây)
