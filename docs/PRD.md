# PRD — Flowbyte ETL Tool

**Version:** 1.0  
**Ngày:** 2026-04-24  
**Author:** Duy Ngo  
**Status:** Sprint 1 — In Progress

---

## 1. Tổng quan sản phẩm

Flowbyte là một ETL (Extract → Transform → Load) tool chạy trên VPS, có nhiệm vụ đồng bộ dữ liệu thương mại điện tử từ **Haravan API** sang **PostgreSQL**. Dữ liệu sau khi sync được dùng cho reporting, phân tích hành vi khách hàng, và vận hành nội bộ.

**Tech stack:** Python 3.12+, PostgreSQL 15+, APScheduler, SQLAlchemy, httpx, structlog

---

## 2. Bối cảnh & động lực

- Haravan không cung cấp native data export sang PostgreSQL
- Team cần data để build dashboard, chạy query adhoc, và tích hợp với các công cụ BI
- Giải pháp hiện tại (export CSV thủ công) tốn thời gian và error-prone
- Flowbyte tự động hóa toàn bộ chu trình này, chạy liên tục 24/7 không cần can thiệp thủ công

---

## 3. Người dùng & vai trò

| Vai trò | Mô tả |
|---|---|
| **Data Analyst** | Dùng dữ liệu trong PostgreSQL để phân tích, không tương tác trực tiếp với Flowbyte |
| **DevOps / Operator** | Cài đặt, configure, vận hành Flowbyte trên VPS qua CLI |
| **Developer** | Mở rộng resource mới, sửa lỗi, viết test |

---

## 4. Phạm vi Sprint 1

Sprint 1 xây dựng nền tảng core: schema setup, API client, sync engine, scheduler, security, và CLI. Chưa có web UI.

**Resources được sync (Phase 1):**
- `orders` + `order_line_items`
- `customers`
- `products` + `variants`
- `inventory_levels`
- `locations`

---

## 5. User Stories

### US-001 — Internal DB Schema Setup

**As a** developer,  
**I want** một schema chuẩn trong internal DB để lưu trạng thái pipeline, credentials, sync history, và logs,  
**So that** mọi thành phần của Flowbyte có thể ghi và đọc dữ liệu kiểm soát một cách nhất quán.

**Acceptance Criteria:**
- [ ] Migration `001` tạo đủ 11 bảng: `pipelines`, `credentials`, `sync_requests`, `sync_logs`, `sync_checkpoints`, `sync_runs`, `validation_results`, `scheduler_heartbeat`, `deployment_events`, `backup_events`, `master_key_metadata`
- [ ] Materialized view `sync_rollup_daily` tồn tại sau migration, GROUP BY theo timezone `Asia/Ho_Chi_Minh`
- [ ] `sync_checkpoints.last_id` là `bigint` (composite cursor tie-breaker)
- [ ] `sync_requests.recovery_count` là `smallint`
- [ ] Partial index `idx_sync_requests_pending` chỉ index `status = 'pending'`
- [ ] Partial index `idx_sync_logs_errors` chỉ index `level IN ('ERROR', 'CRITICAL')`
- [ ] `downgrade` drop view trước khi drop tables
- [ ] Migration roundtrip: upgrade → downgrade → upgrade thành công

---

### US-002 — Security: Credential Encryption

**As a** operator,  
**I want** credentials (Haravan access token, Postgres password) được mã hóa trước khi lưu vào DB,  
**So that** nếu DB bị dump, attacker không đọc được plaintext credentials.

**Acceptance Criteria:**
- [ ] Master key 256-bit được tạo bằng `os.urandom(32)`, lưu tại `/etc/flowbyte/master.key` với permissions `600`
- [ ] Mã hóa dùng AES-256-GCM với nonce ngẫu nhiên 12 bytes mỗi lần encrypt
- [ ] Associated data (AAD) binding với credential `ref` để chống swap attack
- [ ] `ciphertext` được serialize dưới dạng `base64(nonce || tag || ciphertext)` lưu vào cột `text`
- [ ] Decrypt thất bại nếu AAD sai (wrong ref) hoặc ciphertext bị tamper
- [ ] Bootstrap kiểm tra master key đã tồn tại trước khi overwrite (không ghi đè)
- [ ] `flowbyte creds set <ref> --kind haravan` prompt shop_domain + access_token, validate format domain trước khi lưu
- [ ] `flowbyte creds set <ref> --kind postgres` prompt password
- [ ] `flowbyte creds list` liệt kê ref + kind + updated_at (không hiện ciphertext)
- [ ] `flowbyte creds delete <ref>` xóa với confirm prompt

---

### US-003 — Haravan API Client với Rate Limiting

**As a** developer,  
**I want** một HTTP client kết nối Haravan API có rate limiting, retry, và keyset pagination,  
**So that** sync không bị 429 và dữ liệu được lấy đầy đủ kể cả khi source có nhiều records.

**Acceptance Criteria:**
- [ ] Token bucket: capacity 80, leak rate 4 tokens/giây, safety margin 70 — block trước khi đạt 70
- [ ] Token bucket sync từ header `X-Haravan-Api-Call-Limit` (format `"45/80"`) sau mỗi response
- [ ] Cold-start prime: gọi `GET /shop.json` khi init client để set initial token count; fallback 50/80 nếu fail
- [ ] Retry tối đa 5 lần với exponential backoff (min 2s, max 60s) cho `429`, `5xx`, network errors
- [ ] Không retry cho `4xx` client errors (trừ 429)
- [ ] Keyset pagination bằng composite cursor `(updated_at, id)`, page size 250
- [ ] 5-minute overlap look-back cho incremental sync để catch delayed records
- [ ] Pagination detect stuck: full page nhưng không có record mới → log warning và dừng

---

### US-004 — Pipeline Config & CLI Management

**As a** operator,  
**I want** quản lý pipeline qua file YAML và CLI commands,  
**So that** tôi có thể configure, enable/disable pipeline mà không cần sửa code.

**Acceptance Criteria:**
- [ ] `flowbyte bootstrap` tạo master key và config templates lần đầu
- [ ] `flowbyte init <name>` tạo pipeline YAML template
- [ ] `flowbyte validate <name>` test kết nối Haravan + Postgres (< 5s), báo schedule collision
- [ ] `flowbyte enable <name>` upsert pipeline vào DB, bật scheduler
- [ ] `flowbyte disable <name>` tắt scheduler
- [ ] `flowbyte delete <name>` xóa pipeline với confirm (--force để skip)
- [ ] `flowbyte list` hiển thị tất cả pipelines với trạng thái enabled
- [ ] Pipeline YAML có các section: `haravan`, `destination`, `resources` với per-resource schedule, sync_mode, transform
- [ ] Validate cron expression hợp lệ, detect schedule collision (cảnh báo nếu 2+ resource cùng minute)
- [ ] `variants` không bị flag collision với `products` (paired extractor)
- [ ] `TransformConfig.rename` không cho phép duplicate target column names
- [ ] Không thể skip cột `id`

---

### US-005 — Sync Engine: Transform & Load

**As a** developer,  
**I want** một sync engine chuyển JSON từ Haravan sang PostgreSQL rows với đầy đủ flattening và type casting,  
**So that** data trong DB sẵn sàng query mà không cần xử lý thêm.

**Acceptance Criteria:**
- [ ] Mỗi record được flatten theo quy tắc per-resource: flat fields, nested objects (shipping_address → shipping_city/province/...), nested arrays → JSONB
- [ ] Mỗi row có meta columns: `_raw` (JSON gốc), `_synced_at`, `_sync_id`, `_deleted_at`
- [ ] `order_line_items`, `variants`, `inventory_levels` KHÔNG có `_deleted_at` (non-soft-delete tables)
- [ ] `apply_transform` raise `InvalidRecordError` nếu record thiếu `id` (kể cả `id=0`)
- [ ] `type_override` cho phép ép kiểu field sang `numeric`, `integer`, `boolean`, `timestamp`; trả về `None` nếu cast thất bại
- [ ] `TransformConfig.rename` đổi tên column output; `skip` bỏ qua field
- [ ] Upsert (INSERT ... ON CONFLICT DO UPDATE) theo primary key
- [ ] Soft-delete trên full_refresh: set `_deleted_at` cho records không còn trong source — chỉ thực hiện nếu delete < 5% tổng rows
- [ ] `sync_runs` ghi lại: fetched_count, upserted_count, skipped_invalid, soft_deleted_count, rows_before, rows_after, duration_seconds
- [ ] Checkpoint lưu `(last_updated_at, last_id)` sau mỗi sync thành công
- [ ] Incremental mode dùng checkpoint làm cursor, full_refresh ignore checkpoint

---

### US-006 — Scheduler & Daemon

**As a** operator,  
**I want** một background daemon tự động chạy sync theo lịch cron,  
**So that** dữ liệu luôn được cập nhật mà không cần trigger thủ công.

**Acceptance Criteria:**
- [ ] APScheduler với `max_workers=1` để tránh concurrent sync cùng resource
- [ ] Mỗi resource có 2 schedule: incremental (mặc định mỗi 2h staggered) + weekly full refresh (Chủ nhật sáng)
- [ ] Default schedules staggered để tránh misfire: orders@0, customers@5, products@10, inventory@15, locations@20 (phút)
- [ ] Reconciler poll DB mỗi 10s, sync lại schedule nếu pipeline config thay đổi
- [ ] `scheduler_heartbeat` cập nhật mỗi 30s; operator có thể check health
- [ ] `flowbyte sync run <pipeline> <resource>` trigger manual sync
- [ ] Manual sync qua `sync_requests` table (queue-based)
- [ ] Daemon graceful shutdown: chờ job hiện tại hoàn thành trước khi exit

---

### US-007 — Structured Logging

**As a** operator,  
**I want** logs được ghi vào DB và stdout với đầy đủ context,  
**So that** tôi có thể debug vấn đề và theo dõi sync history.

**Acceptance Criteria:**
- [ ] Dùng `structlog` với JSON output
- [ ] Log sink ghi vào bảng `sync_logs` trong internal DB
- [ ] Retention: xóa logs cũ hơn 14 ngày
- [ ] Sensitive fields (`token`, `password`, `secret`, `ciphertext`, v.v.) bị redact trong logs
- [ ] Mỗi log event có `sync_id`, `pipeline`, `resource`, `level`, `event`, `timestamp`
- [ ] `exc_info=True` cho ERROR logs để capture traceback
- [ ] Batch insert logs (không ghi từng record riêng lẻ)
- [ ] Nếu DB sink fail, log vẫn ra stdout (không crash)

---

### US-008 — Post-Sync Validation

**As a** data analyst,  
**I want** kiểm tra tự động chất lượng dữ liệu sau mỗi sync,  
**So that** tôi biết khi nào dữ liệu có vấn đề mà không cần query thủ công.

**Acceptance Criteria:**
- [ ] **R1 — Fetch/Upsert Parity:** Cảnh báo nếu > 1% records bị skip (fetched >> upserted)
- [ ] **R2 — Volume Sanity:** Cảnh báo nếu incremental fetch về 0 liên tiếp 3 lần (gợi ý token hết hạn)
- [ ] **R3 — Weekly Full Drift:** Cảnh báo nếu full refresh count lệch > 5% so với lần trước; fail nếu > 20%
- [ ] **R4 — Soft Delete Sanity:** Block soft-delete sweep nếu sẽ xóa > 5% tổng rows (full_refresh)
- [ ] Kết quả validation lưu vào bảng `validation_results`
- [ ] `failed` validation gửi alert (nếu alerting được cấu hình)

---

### US-009 — Alerting (Telegram)

**As a** operator,  
**I want** nhận thông báo Telegram khi sync fail hoặc validation cảnh báo,  
**So that** tôi biết ngay khi có vấn đề mà không cần check log thủ công.

**Acceptance Criteria:**
- [ ] Alert khi: sync failed, validation failed/warning, scheduler crash
- [ ] Deduplication: không gửi cùng một loại alert quá 1 lần trong 30 phút
- [ ] Retry 3 lần khi Telegram API fail; log lỗi và tiếp tục nếu vẫn fail (không crash daemon)
- [ ] `flowbyte alerting test` gửi test message để verify config
- [ ] `flowbyte alerting enable/disable` bật tắt alerting
- [ ] Bot token và chat_id lưu trong `config.yml` (không phải credential store)

---

### US-010 — Observability & Retention

**As a** operator,  
**I want** xem metrics sync và tự động dọn dẹp dữ liệu cũ,  
**So that** DB không bị đầy và tôi có thể theo dõi hiệu suất.

**Acceptance Criteria:**
- [ ] `flowbyte obs summary` hiển thị tổng hợp sync 24h: counts, duration, failure rate
- [ ] `flowbyte obs logs <pipeline>` xem sync logs gần nhất
- [ ] `flowbyte obs runs <pipeline>` xem lịch sử sync runs
- [ ] Retention cleanup tự động chạy hàng ngày: xóa `sync_logs` > 14 ngày
- [ ] Materialized view `sync_rollup_daily` refresh sau mỗi full refresh cycle

---

## 6. Non-functional Requirements

| Requirement | Target |
|---|---|
| Sync latency | Incremental sync ≤ 2 phút sau khi schedule fire |
| Throughput | ≥ 1000 records/phút với rate limit safety margin |
| Reliability | Retry tối đa 5 lần trước khi mark sync failed |
| Security | Credentials mã hóa AES-256-GCM, master key chmod 600 |
| Timezone | Tất cả timestamps lưu UTC; rollup theo Asia/Ho_Chi_Minh |
| Test coverage | Unit tests ≥ 90% cho core logic; integration tests cho schema |

---

## 7. Out of Scope (Sprint 1)

- Web UI / API server
- Multi-tenant support
- Custom resource mapping (ngoài 6 Phase 1 resources)
- Real-time streaming (Webhook)
- Backup & restore automation
- Phiên bản Haravan API mới hơn v1

---

## 8. Lịch sử thay đổi

| Version | Ngày | Thay đổi |
|---|---|---|
| 1.0 | 2026-04-24 | Phiên bản đầu, Sprint 1 scope |
