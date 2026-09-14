# Customer360 ETL Data Lakehouse — Tài liệu

> Mục tiêu: trình bày project từ A→Z ở **mức công nghệ và cơ chế**, dùng cho phỏng vấn / CV.
> Kiến trúc: **Data Lakehouse** theo **Medallion (Bronze–Silver–Gold)** trên **PySpark + Delta Lake**, chạy **batch**.

---

## 0. Elevator pitch

> "Xây một **ETL data lakehouse** để dựng **chân dung khách hàng 360 độ (Customer360)** cho một nền tảng truyền hình/giải trí số. Hệ thống lấy hai nguồn hành vi thô — **log tìm kiếm** và **log xem nội dung** — làm sạch, chuẩn hóa rồi tổng hợp thành một bảng duy nhất mô tả mỗi khách: họ **tìm gì**, **xem gì**, **sở thích có đổi theo thời gian không**, **hoạt động mạnh hay yếu**. Em dùng **PySpark** làm engine và **Delta Lake** làm định dạng bảng, tổ chức dữ liệu theo **Bronze–Silver–Gold (Medallion)**. Nhờ Delta, pipeline có **ACID, cập nhật incremental bằng MERGE/upsert, time-travel**. Đầu ra là các data mart (Delta + export CSV) cho BI và marketing dùng để phân khúc khách và gợi ý nội dung."

---

## 1. Bối cảnh & bài toán business

**Customer360 là gì?** Gom mọi mảnh dữ liệu về một khách hàng từ nhiều nguồn khác nhau lại thành **một hồ sơ thống nhất**. Thay vì "khách A ở hệ thống tìm kiếm" và "khách A ở hệ thống xem phim" là hai thực thể rời rạc, ta ghép lại để có cái nhìn toàn diện 360 độ.

**Dữ liệu đến từ một nền tảng OTT/truyền hình số** (dấu hiệu: CHANNEL = truyền hình, KPLUS = K+, FIMS/VOD = phim, CHILD = thiếu nhi, RELAX = giải trí). Hai nguồn hành vi:

1. **Log tìm kiếm (search log)** — mỗi lần người dùng gõ từ khóa sinh một bản ghi: `user_id`, `keyword`, thời gian. Lưu dạng **Parquet** theo thư mục ngày.
2. **Log xem nội dung (content log)** — mỗi phiên xem sinh một bản ghi: hợp đồng thuê bao (`contract`), loại ứng dụng (`AppName`), **tổng thời lượng xem** (`TotalDuration`). Lưu dạng **JSON** theo ngày (export dạng Elasticsearch, field thật nằm trong `_source`).

**Bài toán cần trả lời:**

- Mỗi khách quan tâm chủ đề gì (qua từ khóa)?
- Sở thích có **dịch chuyển** giữa tháng này và tháng sau không? (ví dụ T6 mê Anime, T7 chuyển sang Phim Hàn)
- Họ thực sự **xem** loại nội dung nào nhiều nhất, "khẩu vị" ra sao?
- Họ là khách **hoạt động mạnh hay yếu**?

**Giá trị business:** phân khúc khách hàng (segmentation), gợi ý nội dung (recommendation), cảnh báo rời bỏ (churn signal), định hướng chiến lược nội dung.

---

## 2. Bức tranh kiến trúc — Lakehouse theo Medallion (Bronze–Silver–Gold)

```
                 ┌──────────────── RAW (nguồn) ────────────────┐
                 │  log_search  (parquet, T6 + T7)             │
                 │  log_content (json, theo ngày)              │
                 │  log_search_stream  (landing, file mới)     │
                 └─────────────────────────────────────────────┘
                                     │
            ┌────────── BATCH ───────┴──────── STREAMING (near-realtime)
            ▼                                                  ▼
   ┌─────────────────────┐                        ┌──────────────────────────┐
   │ BRONZE (Delta)      │  raw + metadata,       │ stream: landing parquet  │
   │  content_logs       │  append-only           │  ──▶ bronze/search_logs  │
   │  search_logs        │◄───────────────────────│  (file-source streaming) │
   └──────────┬──────────┘                        └──────────────────────────┘
              ▼                                    ┌──────────────────────────┐
   ┌─────────────────────┐                         │ stream: bronze ──▶ silver│
   │ SILVER (Delta)      │  đã làm sạch / chuẩn    │  (foreachBatch micro-    │
   │  content_events     │  hóa, event-level       │   batch)                 │
   │  search_clean       │◄────────────────────────└──────────────────────────┘
   └──────────┬──────────┘
              ▼
   ┌──────────────────────────────────────────────────┐
   │ GOLD (Delta, MERGE / upsert incremental)          │
   │  customer_content_profile (key: contract)         │
   │  customer_search_trend    (key: user_id)          │
   │  customer360_profile  ⭐  (key: user_id)          │
   │  _exports/mart_customer360_profile.csv (cho BI)   │
   └──────────────────────────────────────────────────┘
```

**Ý nghĩa 3 tầng:**

- **Bronze** — log gốc giữ nguyên, chỉ thêm metadata kỹ thuật. Append-only.
- **Silver** — Dữ liệu sạch/chuẩn hóa ở mức từng sự kiện.
- **Gold** — data mart business dùng trực tiếp.

**Điểm khác biệt so với pipeline "thường": cả 3 tầng đều là bảng Delta Lake**, không phải CSV/Parquet rời rạc — đây là thứ biến nó thành **lakehouse đúng nghĩa** (xem mục 4.2).

**Tại sao chia 3 tầng?** Truy vết & sửa lỗi (sai thì lần ngược từng tầng), tái sử dụng (Silver sạch nuôi nhiều bài Gold khác nhau), tách bạch trách nhiệm.

---

## 3. Luồng chạy — batch 7 bước + 2 luồng streaming

### 3.1. Pipeline BATCH (cách chạy chính, ra kết quả "đúng nhất")

`run_pipeline.py` chạy tuần tự theo medallion, mỗi job là một Spark application riêng:

| Nhóm   | Job                      | Việc làm                                                          | Ghi vào (Delta)                 |
| ------ | ------------------------ | ----------------------------------------------------------------- | ------------------------------- |
| Bronze | ingest_content_logs      | Nạp JSON content, flatten `_source`, lấy `event_date` từ tên file | `bronze/content_logs`           |
| Bronze | ingest_search_logs       | Nạp parquet search, lấy `event_date` từ tên folder                | `bronze/search_logs`            |
| Silver | clean_content            | Map AppName → content_type (6 nhóm)                               | `silver/content_events`         |
| Silver | clean_search             | Làm sạch keyword tiếng Việt, bỏ null user_id/keyword              | `silver/search_clean`           |
| Gold   | customer_content_profile | Pivot thời lượng + khẩu vị + hoạt động → **upsert**               | `gold/customer_content_profile` |
| Gold   | customer_search_trend    | Top keyword + category + cờ dịch chuyển → **upsert**              | `gold/customer_search_trend`    |
| Gold   | customer360_profile      | Ghép qua bridge table → **upsert** + export CSV                   | `gold/customer360_profile`      |

Hai nhánh độc lập (tìm kiếm vs xem nội dung) hội tụ ở job cuối.

### 3.2. Luồng STREAMING (near-realtime, cho log tìm kiếm)

Hai job thường trú tạo thành chuỗi `landing → bronze → silver`:

- **`stream_search_logs.py`**: Spark Structured Streaming theo dõi thư mục landing; có file parquet mới rơi vào là tự nạp vào `bronze/search_logs`.
- **`stream_clean_search.py`**: đọc `bronze/search_logs` như một **stream**, làm sạch theo từng micro-batch (dùng lại logic của `clean_search.py`) rồi ghi tiếp vào `silver/search_clean` qua `foreachBatch`.

Gold được làm tươi bằng cách chạy lại nhóm gold theo lịch (chưa tự động hóa — xem mục 8).

---

## 4. Mổ xẻ từng công nghệ (vai trò — cơ chế — tại sao chọn — ưu/nhược)

### 4.1. PySpark / Apache Spark — engine xử lý dữ liệu

**Vai trò:** "Động cơ" làm toàn bộ việc nặng — đọc log, làm sạch, nhóm, đếm, ghép bảng, ghi Delta.

**Cơ chế (ẩn dụ):** Đếm phiếu bầu cả nước. Một người đếm thì rất lâu; Spark **chia phiếu cho nhiều người đếm song song rồi cộng lại** — xử lý phân tán. Spark còn "lười thông minh": không làm ngay từng lệnh mà ghi nhớ cả kế hoạch (DAG), tối ưu rồi mới chạy khi cần kết quả — **lazy evaluation**.

**Tại sao chọn Spark:**

- So với **Pandas** (nạp hết vào RAM một máy): log hành vi có thể hàng chục–trăm triệu dòng, Pandas dễ hết bộ nhớ; Spark chia partition nên kham được dữ liệu lớn hơn RAM.
- So với **SQL thuần trên warehouse**: Spark linh hoạt hơn cho logic làm sạch text tiếng Việt phức tạp, tích hợp gốc với Delta.
- So với **Flink** (chuyên realtime mili-giây): bài này chủ yếu **batch**, không cần độ trễ cực thấp.

**Ưu / nhược:**

- ✅ Mở rộng được (scale-out); hệ sinh thái SQL/ML; tối ưu tự động qua Catalyst.
- ❌ Khởi động nặng (chạy trên JVM), tốn warm-up — không hợp dữ liệu vài nghìn dòng.
- ❌ **Demo chạy single-node (local mode)** nên chưa khai thác hết phân tán — nhưng code viết chuẩn Spark DataFrame nên đổi cấu hình cụm là chạy đa máy mà không viết lại logic.

### 4.2. Delta Lake — định dạng bảng giao dịch (TRÁI TIM của kiến trúc)

**Vai trò:** Table format cho cả 3 tầng Bronze/Silver/Gold — biến project từ "pipeline batch + Parquet/CSV" thành **data lakehouse đúng nghĩa**.

**Cơ chế (ẩn dụ):** Delta Lake = **Parquet + một cuốn nhật ký giao dịch** (`_delta_log`). Parquet thuần giống một đống file dữ liệu rời; Delta gắn thêm cuốn sổ ghi "ai ghi cái gì, lúc nào, phiên bản mấy".

**Delta cho project này những gì:**

- **ACID transaction:** ghi nguyên tử — không còn cảnh job chết giữa chừng để lại file hỏng.
- **MERGE / upsert (cập nhật incremental):** chỉ cập nhật bản ghi thay đổi thay vì ghi đè toàn bộ (xem 4.6).
- **Time travel:** xem lại bảng ở phiên bản cũ (`VERSION AS OF`), tiện audit/rollback.
- **Schema evolution:** thêm cột không vỡ pipeline.

**Ưu / nhược:**

- ✅ Đem chất "database" (ACID, upsert, version) vào data lake giá rẻ trên file.
- ❌ Thêm một lớp phụ thuộc (`delta-spark`) phải khớp version với Spark; sinh file log/metadata cần dọn định kỳ (`OPTIMIZE`, `VACUUM`).
- _Họ hàng:_ **Apache Iceberg**, **Apache Hudi** — cùng nhóm "lakehouse table format". Chọn Delta vì tích hợp PySpark mượt nhất và dễ chạy local.

### 4.3. Spark Structured Streaming — luồng near-realtime

**Vai trò:** Nạp log tìm kiếm mới **liên tục** từ thư mục landing vào bronze, rồi làm sạch tiếp sang silver, mà không cần chạy lại batch.

**Cơ chế (ẩn dụ):** Thay vì "mỗi tháng gom hết rồi xử lý một lần" (batch), streaming giống **băng chuyền**: dữ liệu tới tới đâu xử lý tới đó theo từng **micro-batch** (lô nhỏ vài giây). Spark dùng **checkpoint** (điểm lưu tiến độ) để nhớ đã xử lý file nào → **exactly-once**, không nạp trùng kể cả khi job khởi động lại.

**Tại sao "near-realtime" chứ không "true realtime":** Structured Streaming xử lý theo **micro-batch** (độ trễ giây), không phải từng-bản-ghi tức thời như event-streaming thuần (Flink/Kafka Streams). Với Customer360, độ trễ giây→phút là quá đủ.

**Hai chế độ trigger (đổi qua cấu hình):**

- _microbatch:_ chạy liên tục, mỗi khoảng thời gian (vd 30s) quét dữ liệu mới → near-realtime.
- _availableNow:_ xử lý hết dữ liệu hiện có rồi dừng → incremental batch theo lịch (giống cron) nhưng vẫn exactly-once nhờ checkpoint.

**Ưu / nhược:**

- ✅ Cùng API DataFrame với batch nên không phải học engine mới; tái dùng đúng logic làm sạch (`stream_clean_search.py` gọi lại logic của `clean_search.py` qua `foreachBatch`).
- ❌ Hiện dùng **file-source** (thư mục landing), chưa có message broker; muốn realtime "thật" cần thêm **Kafka** ở phía trước (đây là hướng phát triển — xem mục 8).

### 4.4. Parquet — định dạng cột nằm bên dưới

**Vai trò:** Định dạng của **log tìm kiếm đầu vào**, và là **lớp lưu trữ vật lý bên dưới Delta** (Delta = Parquet + log).

**Cơ chế (ẩn dụ):** CSV lưu **theo dòng**; Parquet lưu **theo cột**. Giống xếp sách theo thể loại thay vì theo người mượn: cần "trinh thám" thì lấy đúng kệ, khỏi lục cả thư viện.

**Tại sao Parquet:** đọc nhanh khi chỉ cần vài cột (project chỉ cần `user_id`, `keyword`), nén tốt, giữ schema/kiểu, hỗ trợ **predicate pushdown**.

**Ưu / nhược:** ✅ lý tưởng cho phân tích đọc chọn cột trên dữ liệu lớn; ❌ không đọc bằng mắt thường, không hợp append từng dòng lẻ.

### 4.5. JSON — định dạng của log xem nội dung

**Vai trò:** Content log lưu JSON theo từng ngày.

**Cơ chế:** JSON **lồng nhau (nested)** kiểu xuất từ Elasticsearch (dữ liệu thật trong khối `_source`). Spark suy ra cấu trúc rồi **trải phẳng (flatten)** thành cột.

**Tại sao dùng JSON ở đây:** **không phải lựa chọn chủ động mà là định dạng gốc hệ thống xuất ra** — tình huống thực tế: data engineer hiếm khi chọn được format nguồn. Vì JSON nặng và parse chậm, pipeline **đọc JSON thô ở bronze rồi nhanh chóng chuyển sang Delta** ở các tầng sau.

### 4.6. MERGE / upsert — cập nhật incremental

**Vai trò:** Các bảng **Gold** không ghi đè toàn bộ; được **MERGE (upsert)** theo khóa: bản ghi đã có thì **update**, bản ghi mới thì **insert** (`whenMatchedUpdateAll` / `whenNotMatchedInsertAll`).

**Cơ chế (ẩn dụ):** Giống cập nhật danh bạ điện thoại: tên đã có thì sửa số mới, tên chưa có thì thêm dòng — **không xóa cả danh bạ rồi nhập lại từ đầu**. Khóa upsert: `user_id` cho search_trend & customer360, `contract` cho content_profile.

**Tại sao quan trọng:** biến pipeline thành **incremental** thật sự — chạy lại chỉ động vào phần thay đổi, nhanh và an toàn. Đồng thời cho **idempotency** (chạy lại không nhân đôi dữ liệu) "miễn phí".

> Lưu ý: tầng **Silver** trong project này dùng `mode="overwrite"` (full rebuild từ Bronze mỗi lần chạy) để đảm bảo idempotent đơn giản; **Gold** mới dùng MERGE incremental thật sự.

### 4.7. Window function — phép tính "top N trong mỗi nhóm"

**Vai trò:** tìm **từ khóa được tìm nhiều nhất của mỗi user mỗi tháng** (top-1 mỗi nhóm, dùng `row_number`).

**Cơ chế (ẩn dụ):** Xếp hạng trong từng lớp học. `group by` chỉ cho **con số tổng hợp** của cả lớp (điểm cao nhất 9.5) nhưng **không cho biết ai**. Window **chia bảng thành từng "khung"** (mỗi user-tháng một khung), sắp xếp trong khung theo số lần tìm, **đánh số thứ tự** rồi lấy hạng 1 — kèm **đầy đủ thông tin dòng**.

**Tại sao window thay vì group by:** group by gộp mất chi tiết dòng; để lấy "dòng quán quân kèm mọi thuộc tính trong mỗi nhóm", window là cách chuẩn mực, thay cho self-join lòng vòng.

### 4.8. UDF — hàm tự định nghĩa để phân loại từ khóa thành category

**Vai trò:** Gán mỗi từ khóa vào một **danh mục** (Anime, Phim Hàn, Phim Trung, Thể thao, Tâm linh… hoặc "Khác") dựa trên bảng mapping `keyword_category_mapping.csv`.

**Cơ chế:** Không khớp y hệt mà **chuẩn hóa** (bỏ dấu, viết thường, bỏ ký tự lạ), **ưu tiên khớp cụm dài trước** (longest-match-first, để "phim hàn quốc" được nhận trước quy tắc chung chung). Logic này phức tạp nên đóng gói thành UDF.

**Tradeoff:**

- Lần đầu thử **exact-match broadcast join** → ~73% từ khóa rơi vào "Khác" (không khớp). Chuyển sang **UDF với substring/longest-match-first** → coverage cải thiện đáng kể (còn ~61% "Khác", nhưng phần khớp được đúng ngữ cảnh hơn).
- ✅ Biểu đạt được logic văn bản phức tạp mà SQL thuần khó viết.
- ❌ **UDF chậm:** Spark phải chuyển dữ liệu qua lại JVM↔Python từng dòng, Catalyst **không nhìn vào trong UDF** (hộp đen).
- **Cải tiến nếu được hỏi:** dùng **pandas UDF (vector hóa)** để nhanh hơn, hoặc thay static mapping bằng **LLM classification** (đã cân nhắc dùng Ollama nhưng chọn static mapping vì đơn giản và đủ dùng cho phạm vi project).

### 4.9. Bridge table — chìa khóa nối hai thế giới dữ liệu

**Vai trò:** **Mấu chốt kiến trúc** của Customer360. Log tìm kiếm định danh bằng `user_id`, log xem nội dung định danh bằng `contract`. **Hai nguồn không có khóa chung.** Bridge table (`reference/customer_key_bridge.csv`) là **bảng ánh xạ `user_id ↔ contract`** để ghép được.

**Đây là bài toán kinh điển: "Identity Resolution / Entity Resolution"** — luôn xuất hiện khi dựng Customer360 thật, vì mỗi hệ thống nội bộ có định danh riêng.

**Sự thật cần thành thật (Known limitation):** bridge table trong project là **dữ liệu sinh tổng hợp** (~30% tỷ lệ khớp ngẫu nhiên), chưa có ánh xạ định danh thật. Do đó phần lớn khách chưa ghép được sang content → các cột content trong bảng cuối để trống (NULL) với các user không match.

### 4.10. CSV export — serving layer đơn giản

Sau khi tính xong, bảng gold cuối cùng (`customer360_profile`) được **export thêm một file CSV phẳng** (`gold/_exports/`) cho công cụ BI/người dùng không đọc được Delta trực tiếp. Đây là **đầu ra**, không phải định dạng trung gian giữa các tầng.

### 4.11. Orchestrator (`run_pipeline.py`)

**Vai trò:** Script điều phối chạy 7 job batch đúng thứ tự phụ thuộc (Bronze → Silver → Gold), mỗi bước là một tiến trình Spark sạch.

---

## 5. Luồng ETL chi tiết

### Bronze — Nạp dữ liệu thô vào lakehouse

- **Content:** đọc từng file JSON theo ngày, trải phẳng `_source.*`, lấy `event_date` từ tên file bằng regex (`\d{8}\.json`), gắn `source_file` + `ingest_ts`, append vào `bronze/content_logs`, partition theo `event_date`.
- **Search:** đọc parquet theo thư mục ngày, lấy `event_date` từ tên folder, gắn metadata, append vào `bronze/search_logs`.
- _(Streaming song song:_ file parquet mới ở landing được `stream_search_logs.py` nạp thẳng vào `bronze/search_logs`.)\*

> **Vì sao đáng nói:** bronze append-only + metadata nguồn là chuẩn lakehouse — luôn truy được "dòng này đến từ file nào, lúc nào".

### Silver — Làm sạch & chuẩn hóa (event-level)

**Content clean (`clean_content.py`):** đọc `bronze/content_logs`, **map AppName → content_type** (6 nhóm tiếng Việt: CHANNEL→Truyền hình, RELAX→Giải trí, CHILD→Thiếu nhi, FIMS/VOD→Phim truyện, KPLUS/SPORT→Thể thao, còn lại→Khác), ghi vào `silver/content_events` bằng `mode="overwrite"` (idempotent full rebuild).

**Search clean (`clean_search.py`):** đọc `bronze/search_logs`, bỏ dòng thiếu `user_id`/`keyword`, **chuẩn hóa keyword** bằng regex giữ dấu tiếng Việt (về chữ thường, gỡ ký tự đặc biệt, nén khoảng trắng, giữ nguyên dấu), ghi vào `silver/search_clean` — cũng `mode="overwrite"`.

> Việc đếm/tổng hợp **không làm ở silver** — silver giữ chi tiết để nhiều bài Gold tái dùng.

### Gold — Tổng hợp thành data mart (Delta + upsert)

**1) customer_content_profile** (khóa `contract`):

1. **Pivot** thời lượng theo các nhóm nội dung (dùng `F.greatest` để tìm loại xem nhiều nhất).
2. Tính `most_watch_type`, `taste_profile` (khẩu vị), `active_days` & `activity_level`.
3. **MERGE upsert** vào `gold/customer_content_profile`.

**2) customer_search_trend** (khóa `user_id`):

1. Đếm tần suất từ khóa theo (user, tháng); chọn **top-1 mỗi user mỗi tháng** bằng window `row_number`.
2. Gán category cho keyword bằng UDF (substring/longest-match-first).
3. Tạo tín hiệu insight: `keyword_changed_flag`, `category_shift_flag`, `category_transition` (ví dụ: "Anime → Phim Hàn Quốc").
4. **MERGE upsert** vào `gold/customer_search_trend`.

**3) customer360_profile** ⭐ (khóa `user_id`):

1. Lấy `customer_search_trend` làm **trục chính**, **left join** với bridge table để gắn `contract`.
2. **Left join** tiếp với `customer_content_profile` theo `contract`.
3. Gộp thành **một bảng duy nhất**, **MERGE upsert** vào `gold/customer360_profile`, rồi **export CSV** cho BI.

> Chọn **left join** (không inner) là chủ đích: **giữ toàn bộ khách có dữ liệu tìm kiếm**, kể cả khi chưa ghép được content — thà thông tin một nửa còn hơn loại bỏ khách.

---

## 6. Kết quả cuối cùng "đọc" ra điều gì?

Mỗi dòng bảng cuối là **một khách hàng**:

- **Tìm gì:** từ khóa top theo tháng + số lần.
- **Thuộc thể loại nào:** category theo tháng.
- **Sở thích có đổi không:** đổi từ khóa? đổi thể loại? chuyển từ đâu sang đâu? → ví dụ: "Anime → Phim Hàn Quốc", "Anime → Khác", "Tâm linh → Khác".
- **Xem gì & mạnh không** (khi ghép được content): loại xem nhiều nhất, khẩu vị, mức độ hoạt động.

**Câu chuyện business:**

- Khách **đổi thể loại** → cơ hội gợi ý nội dung mới đúng hướng dịch chuyển.
- Khách **giữ thể loại + hoạt động cao** → trung thành, nên giữ chân/upsell.
- Khách **hoạt động thấp** → nguy cơ rời bỏ, cần tái kích hoạt.

**Nhờ Delta, còn có thể:** xem lại bảng ở phiên bản trước bằng **time travel**, và audit lịch sử thay đổi (`DESCRIBE HISTORY`).

---

## 7. Known limitations (nói thẳng khi phỏng vấn)

- **Bridge table là dữ liệu sinh tổng hợp** (~30% tỷ lệ khớp ngẫu nhiên) — chưa có ánh xạ định danh `user_id ↔ contract` thật từ hệ thống. Đây là mô phỏng bài toán identity resolution, không phải giải pháp production.
- **Coverage phân loại category còn ~61%** (phần còn lại "Khác") — do dùng static keyword mapping thay vì LLM/NLP classification, là đánh đổi thực dụng phù hợp phạm vi project entry-level.
- **Chạy single-node (local mode)** — chưa test trên cluster nhiều máy, dù code viết chuẩn DataFrame API nên về lý thuyết scale ngang được không cần đổi logic.

## 8. Định hướng mở rộng (chưa triển khai, chỉ là ý tưởng kiến trúc)

- **Kafka** làm message broker phía trước luồng streaming hiện tại (đang dùng file-source, chưa phải "true realtime").
- **Serving layer qua MySQL/JDBC** để dashboard tra cứu theo khóa nhanh hơn Delta.
- **Apache Airflow** thay cho `run_pipeline.py` thủ công — lập lịch, quản lý phụ thuộc (kể cả job streaming), retry, cảnh báo.
- **Search Behavior Drift**: đo mức độ dịch chuyển sở thích bằng cosine similarity giữa vector từ khóa T6 và T7 thay vì chỉ so sánh top-1 keyword.
