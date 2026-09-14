# Customer360 ETL Data Lakehouse

ETL data lakehouse dựng **chân dung khách hàng 360 độ (Customer360)** cho một nền tảng OTT/truyền hình số, từ hai nguồn hành vi thô: **log tìm kiếm** và **log xem nội dung**.

Pipeline dùng **PySpark** làm engine xử lý và **Delta Lake** làm định dạng bảng, tổ chức theo kiến trúc **Medallion (Bronze → Silver → Gold)**, hỗ trợ cả 2 luồng **Unified Batch & Near-Realtime Streaming** với **MERGE/upsert incremental** ở tầng Gold.

Tài liệu kỹ thuật đầy đủ (kiến trúc, cơ chế từng công nghệ, hạn chế đã biết): xem [`docs/customer360-etl-data-lakehouse.md`](docs/customer360-etl-data-lakehouse.md).

## Kiến trúc Medallion

```
   RAW                         BRONZE (Delta)        SILVER (Delta)         GOLD (Delta, upsert)
   log_search  (parquet) ───▶  search_logs   ───▶    search_clean    ───┐
   log_content (json)    ───▶  content_logs  ───▶    content_events  ───┤
   log_search_stream ──(stream)─▶ (near-realtime)                       │
                                                                        ▼
                                         customer_search_trend  (key: user_id)
                                         customer_content_profile (key: contract)
                                         customer360_profile ⭐  (key: user_id)
                                         _exports/mart_customer360_profile.csv
```

- **Bronze** — log thô, append-only, kèm metadata (ingest_ts, source_file).
- **Silver** — đã làm sạch / chuẩn hóa ở mức event-level.
- **Gold** — các data mart phục vụ BI, cập nhật bằng MERGE/upsert.

## Cấu trúc thư mục

```
lakehouse/
├── config.py                    # đường dẫn, tên bảng, business key, tham số
├── common.py                    # read_delta, upsert_delta, export_single_csv,...
├── spark_session.py             # khởi tạo SparkSession có Delta Lake
├── run_pipeline.py              # orchestrator chạy tuần tự các job batch
├── bronze/
│   ├── ingest_content_logs.py   # log_content (JSON) -> bronze/content_logs
│   ├── ingest_search_logs.py    # log_search (Parquet) -> bronze/search_logs
│   └── stream_search_logs.py    # streaming: landing (parquet mới) -> bronze/search_logs
├── silver/
│   ├── clean_content.py         # map AppName -> content_type (6 nhóm)
│   ├── clean_search.py          # làm sạch + chuẩn hóa keyword tiếng Việt (batch)
│   └── stream_clean_search.py   # streaming: bronze/search_logs -> silver/search_clean (foreachBatch)
├── gold/
│   ├── customer_content_profile.py  # pivot thời lượng, khẩu vị, mức độ hoạt động
│   ├── customer_search_trend.py     # top keyword/tháng, phân loại category, cờ dịch chuyển
│   ├── customer360_profile.py       # ghép qua bridge table -> mart cuối cùng
│   └── verify_customer360.py        # script kiểm tra schema & validation dữ liệu
reference/
├── customer_key_bridge.csv      # bảng ánh xạ user_id <-> contract (identity resolution)
└── keyword_category_mapping.csv # mapping keyword -> category
requirements.txt
```

## Yêu cầu môi trường

- Python 3.10+ (khuyến nghị dùng virtualenv/conda riêng cho project)
- Java 8/11 (bắt buộc để chạy Spark trên JVM)

## Cài đặt

```bash
# 1. Tạo virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux

# 2. Cài dependencies
pip install -r requirements.txt
```

`requirements.txt`:

```
pyspark==3.5.0
delta-spark==3.1.0
pandas==2.2.2
openpyxl==3.1.2
```

## Chuẩn bị dữ liệu

Sửa đường dẫn raw data trong `lakehouse/config.py` cho đúng máy của bạn:

```python
RAW_CONTENT_DIR = r"đường-dẫn-của-bạn\log_content"   # JSON theo ngày
RAW_SEARCH_DIR  = r"đường-dẫn-của-bạn\log_search"    # Parquet theo folder ngày
```

Bronze/Silver/Gold sẽ tự sinh ra trong `data/` (đã `.gitignore`, không push lên Git).

## Chạy pipeline

### 1. Chạy luồng Batch (Orchestration)

- **Chạy toàn bộ (Bronze → Silver → Gold):**

```
python -m lakehouse.run_pipeline
```

- **Chạy từ một tầng cụ thể (Ví dụ từ Gold):**

```
python -m lakehouse.run_pipeline --from-stage gold
```

- **Chạy từng job riêng lẻ (Khi dev / debug):**

```
# Bronze Layer
python -m lakehouse.bronze.ingest_content_logs
python -m lakehouse.bronze.ingest_search_logs

# Silver Layer
python -m lakehouse.silver.clean_content
python -m lakehouse.silver.clean_search

# Gold Layer (Data Marts)
python -m lakehouse.gold.customer_content_profile
python -m lakehouse.gold.customer_search_trend
python -m lakehouse.gold.customer360_profile
```

- **Kiểm tra dữ liệu sau khi chạy:**

```
python -m lakehouse.gold.verify_customer360
```

_Kết quả cuối: bảng Delta `data/gold/customer360_profile` + file CSV export trong `data/gold/_exports/`._

### 2. Streaming near-realtime (log tìm kiếm)

Bên cạnh batch, project có một nhánh streaming near-realtime cho log tìm kiếm, gồm 2 job thường trú chạy nối tiếp `landing → bronze → silver`:

```
# Job 1: theo dõi thư mục landing, có file parquet mới rơi vào là tự nạp vào bronze/search_logs
python -m lakehouse.bronze.stream_search_logs

# Job 2: đọc bronze/search_logs như một stream, làm sạch từng micro-batch rồi ghi vào silver/search_clean
python -m lakehouse.silver.stream_clean_search
```

Hai job này chạy song song, độc lập với pipeline batch, dùng chung logic làm sạch keyword với `clean_search.py` (qua `foreachBatch`). Checkpoint dùng để đảm bảo exactly-once — không nạp trùng dữ liệu khi job khởi động lại. Gold vẫn được làm tươi bằng cách chạy lại nhóm job Gold theo lịch (`python -m lakehouse.run_pipeline --from-stage gold`).

## Known issues

- **Bridge table (`reference/customer_key_bridge.csv`) là dữ liệu sinh tổng hợp** (~30% tỷ lệ khớp ngẫu nhiên) — chưa có ánh xạ định danh `user_id ↔ contract` thật, nên phần lớn khách chưa ghép được sang content profile trong bảng cuối.
- **Coverage phân loại category keyword còn ~61%** (phần còn lại rơi vào "Khác") — do dùng static mapping (`keyword_category_mapping.csv`, 444 dòng) thay vì LLM classification, là đánh đổi thực dụng cho phạm vi project.

## Resume Highlights

- Built an end-to-end **Customer360 ETL data lakehouse** using **PySpark + Delta Lake** (Medallion architecture)
- Implemented **incremental MERGE/upsert** and **near-realtime streaming** (Spark Structured Streaming)
- Designed Vietnamese text cleaning + keyword→category classification
- Detected user behavior/interest shifts across time for segmentation & recommendation
