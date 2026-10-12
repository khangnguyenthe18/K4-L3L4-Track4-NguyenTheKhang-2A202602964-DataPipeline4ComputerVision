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

### Câu 4

(trả lời ở đây)

### Câu 5

(trả lời ở đây)
