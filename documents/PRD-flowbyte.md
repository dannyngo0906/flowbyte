# PRD — Flowbyte

> Công cụ ETL chuyên biệt giúp Data Analyst tự động đồng bộ dữ liệu từ Haravan API về PostgreSQL trên VPS công ty — tinh gọn như Airbyte nhưng nhẹ và tiết kiệm chi phí hạ tầng.

---

## ① Overview

- **App name:** Flowbyte
- **Tagline:** Công cụ ETL chuyên biệt đồng bộ dữ liệu Haravan → PostgreSQL, CLI-first, self-hosted trên VPS.
- **Problem:**
  - Data Analyst phải **export thủ công** từ admin Haravan, tốn thời gian, không tự động hóa.
  - Tự **viết script Python/Node** gọi API Haravan thì khó bảo trì, dễ lỗi khi schema thay đổi, không có incremental sync.
  - **Airbyte/Fivetran** không có connector Haravan sẵn; tự build custom connector rất tốn công.
  - **Airbyte đã thử nhưng tốn $30/tháng** cho VPS server → phải bỏ, quay lại làm thủ công.
  - **Báo cáo mặc định của Haravan** thiếu chiều phân tích sâu, không đáp ứng nhu cầu BI.
- **Solution:** Flowbyte là ETL tool chuyên biệt cho Haravan:
  - **Connector Haravan API có sẵn** — tự xử lý authentication, rate limit (leaky bucket 80/4rps), pagination, retry.
  - **Đồng bộ tự động về PostgreSQL** với 2 mode: Full Refresh và Incremental Sync.
  - **Scheduler cron-based** không cần can thiệp thủ công.
  - **Alerting Telegram** + CLI observability commands.
  - Self-hosted trên VPS có sẵn — dùng chung với n8n, chi phí target **≤ $9/tháng** (30% Airbyte cũ).
- **Platform & Roadmap:**

```
┌─────────────────────────────────────────────────────────────────┐
│  MVP (Phase 1)   │  Phase 2       │  Phase 3       │  Phase 4   │
│  CLI-only        │  Web UI        │  Advanced      │  More      │
│                  │  + Alerting+   │  transforms    │  resources │
├─────────────────────────────────────────────────────────────────┤
│  6 resources     │  5 resources   │  4 resources   │  7 resources│
│  orders          │  refunds       │  collections   │  users      │
│  customers       │  fulfillments  │  discounts     │  blogs      │
│  products        │  transactions  │  coupons       │  pages      │
│  variants        │  order_risks   │  gift_cards    │  redirects  │
│  inventory_lvls  │  draft_orders  │                │  webhooks   │
│  locations       │                │                │  carriers   │
│                  │                │                │  checkouts  │
└─────────────────────────────────────────────────────────────────┘
```

---

## ② Target User

**Persona chính:**
- **Duy, 35 tuổi, Data Analyst** tại công ty bán lẻ mỹ phẩm, dùng **Haravan Omnichannel** làm hệ thống quản trị bán hàng.
- **Shop profile:**
  - ~76k orders tổng, ~200 orders/ngày
  - 1 shop Haravan duy nhất
  - Dữ liệu SME (< 1M records/resource)
- **Công việc hàng ngày:**
  - Báo cáo doanh thu, phân tích bán hàng, báo cáo tồn kho
  - KPI nhân viên (Haravan không có → phải tải file, matching manual)
  - Báo cáo custom mà Haravan không hỗ trợ
- **Nỗi đau:**
  1. Tốn thời gian export thủ công CSV/Excel từ Haravan
  2. Dữ liệu không real-time, luôn trễ
  3. Matching data Haravan ↔ nội bộ bằng Excel — sai sót, không scale
  4. Lặp lại công việc mỗi tuần/tháng
  5. Đã dùng Airbyte nhưng $30/tháng quá cao → phải bỏ
- **Tool hiện tại:**
  - Haravan Admin (export CSV thủ công)
  - Excel + Google Sheets (xử lý)
  - BigQuery (data warehouse cũ — có thể migrate sang Postgres nội bộ)
  - Airbyte (đã ngưng)
  - SQL cơ bản, terminal thành thạo, quen cron expression
- **VPS hiện có:** 4GB RAM, 2 vCPU, 27GB disk (sẽ nâng cấp ≥ 50GB), đang chạy n8n — Flowbyte sẽ chạy chung qua Docker.

---

## ③ Data Flow Architecture

```
┌──────────────┐      ┌─────────────────────────────────────────────┐       ┌──────────────────┐
│              │      │               FLOWBYTE VPS                   │       │                  │
│   HARAVAN    │      │                                              │       │   DATA ANALYST   │
│   OMNI API   │      │  ┌──────────────────────────────────────┐   │       │                  │
│              │◄─────┤  │ Scheduler (APScheduler)               │   │       │  flowbyte CLI    │
│  leaky bucket│      │  │  - cron per resource                   │   │       │   ├─ init        │
│  80 / 4rps   │      │  │  - weekly full refresh Sun 2am         │   │       │   ├─ status      │
│              │      │  └──────────┬───────────────────────────┘   │       │   ├─ logs        │
└──────────────┘      │             │                                │       │   ├─ run         │
                      │  ┌──────────▼──────────────┐                 │       │   └─ ...         │
                      │  │ Extractor (token bucket) │                 │       │                  │
                      │  │  - pagination            │                 │       └──────────────────┘
                      │  │  - 429 retry w/ backoff  │                 │
                      │  └──────────┬──────────────┘                 │       ┌──────────────────┐
                      │             │                                │       │                  │
                      │  ┌──────────▼──────────────┐                 │       │   TELEGRAM BOT   │
                      │  │ Transformer              │                 │       │                  │
                      │  │  - hybrid flatten/JSONB  │                 │       │  - sync failed   │
                      │  │  - YAML field mapping    │                 ├──────►│  - scheduler dead│
                      │  └──────────┬──────────────┘                 │       │                  │
                      │             │                                │       └──────────────────┘
                      │  ┌──────────▼──────────────┐                 │
                      │  │ Loader                   │                 │       ┌──────────────────┐
                      │  │  - upsert ON CONFLICT    │                 │       │                  │
                      │  │  - atomic checkpoint     │                 │       │  Metabase /      │
                      │  └──────────┬──────────────┘                 │       │  Superset /      │
                      │             │                                │       │  Google Sheet /  │
                      │             │                                │       │  SQL views       │
                      │  ┌──────────▼──────────────────────┐         │       │                  │
                      │  │    PostgreSQL ≥ 14               │◄────────┼───────┤                  │
                      │  │  ┌───────────────┐ ┌──────────┐ │         │       └──────────────────┘
                      │  │  │ destination_db│ │internal_ │ │         │
                      │  │  │  orders       │ │db         │ │         │
                      │  │  │  customers    │ │ credentials│ │         │
                      │  │  │  products     │ │ pipelines  │ │         │
                      │  │  │  ...          │ │ sync_logs  │ │         │
                      │  │  │               │ │ checkpoints│ │         │
                      │  │  └───────────────┘ └──────────┘ │         │
                      │  └──────────────────────────────────┘         │
                      │                                                │
                      │  Docker Compose (shared with n8n)              │
                      │  - Flowbyte container: memory 1.5GB            │
                      └─────────────────────────────────────────────────┘
```

---

## ④ Features & User Stories

### 📋 Resource phases

| Phase | Resources | Sync mode mặc định |
|---|---|---|
| **Phase 1 (MVP)** | `orders`, `customers`, `products`, `variants`, `inventory_levels`, `locations` | Incremental (trừ `inventory_levels` & `locations` → Full Refresh) |
| **Phase 2** | `refunds`, `fulfillments`, `transactions`, `order_risks`, `draft_orders` | Incremental |
| **Phase 3** | `collections`, `discounts`, `coupons`, `gift_cards` | Incremental |
| **Phase 4** | `users`, `blogs`, `pages`, `redirects`, `webhooks`, `carrier_services`, `checkouts` | Full Refresh (ít thay đổi) |

**Weekly Full Refresh:** Áp dụng cho **tất cả resources** (kể cả Incremental) vào **Chủ nhật 2:00 AM** để:
- Bắt record bị update với `updated_at` cũ (Haravan bug hoặc admin backdate)
- Phát hiện record bị xóa trên Haravan → đánh dấu `_deleted_at` (soft delete)

---

### 🟢 MUST (MVP — CLI-only)

---

#### F1: Sync Haravan → PostgreSQL định kỳ
**Story:** Là Data Analyst, tôi muốn tự động đồng bộ 6 resources Phase 1 từ Haravan về PostgreSQL, với lịch sync linh hoạt theo từng resource, để dữ liệu quan trọng (orders, inventory) luôn mới nhất, còn dữ liệu ít đổi (products, locations) không tốn tài nguyên.

**Flow:**
1. User cấu hình Haravan credentials (access_token + shop domain) qua `flowbyte creds set` (lưu encrypted vào internal Postgres)
2. User cấu hình PostgreSQL destination trong YAML
3. User bật/tắt resource trong YAML (Phase 1 default: tất cả ON)
4. Lịch sync mặc định:
   - `orders`, `customers`, `inventory_levels` → **mỗi 2 giờ** (high priority)
   - `products`, `variants` → **mỗi 12 giờ** (medium)
   - `locations` → **mỗi 24 giờ** (low)
   - **Weekly Full Refresh** tất cả resources → Chủ nhật 2:00 AM
   - User override bằng cron expression trong YAML
5. Scheduler trigger sync: **Extract → Transform → Load** upsert vào Postgres
6. Ghi log vào bảng `sync_logs` (internal Postgres)

**Done khi:**
- ✅ Sync được 6 resources Phase 1
- ✅ Mỗi resource cấu hình cron riêng được
- ✅ Dữ liệu khớp với Haravan **±0.1%** sau full refresh đầu (verify `/orders/count.json` vs `SELECT COUNT(*)`)
- ✅ Scheduler không crash **≥ 30 ngày** (`systemctl status flowbyte` uptime)
- ✅ Sync **10k records < 1 phút** (với rate limit 4 req/s × 250 records/page = ~10s)
- ✅ Log riêng cho từng resource trong `sync_logs`

**Edge cases:**
- Haravan 429 → đọc `Retry-After` header, sleep, retry exponential backoff (max 5 lần)
- Token bucket client-side: max 80, refill 4/s — **1 bucket chung cho shop**, không concurrent
- Hai resource trùng giờ sync → queue tuần tự qua token bucket
- Resource fail → chỉ resource đó retry, resource khác chạy bình thường
- Mất kết nối Postgres giữa sync → rollback transaction, retry từ checkpoint cũ
- VPS restart → systemd restart Flowbyte, scheduler khôi phục lịch
- Haravan 401/403 → dừng sync, alert critical Telegram, không retry

---

#### F2: Quản lý Pipeline qua CLI + YAML
**Story:** Là Data Analyst, tôi muốn tạo/sửa/xóa pipeline qua CLI + YAML config, để version control bằng Git và tự quản lý mà không cần chỉnh code Python.

**CLI commands:**
```
flowbyte init <name>            # tạo YAML template /etc/flowbyte/pipelines/<name>.yml
flowbyte creds set <ref>        # nhập credentials interactive → lưu encrypted
flowbyte validate <pipeline>    # test Haravan + Postgres connection (< 5s)
flowbyte list                   # liệt kê pipelines
flowbyte enable <pipeline>      # bật scheduler
flowbyte disable <pipeline>     # tắt scheduler
flowbyte run <pipeline> [--resource orders]  # trigger sync thủ công
flowbyte delete <pipeline>      # xóa pipeline (confirm prompt)
```

**YAML config structure (`/etc/flowbyte/pipelines/shop_main.yml`):**
```yaml
name: shop_main
haravan:
  shop_domain: mycompany.myharavan.com
  credentials_ref: shop_main_creds    # reference tên, credentials thực lưu encrypted trong Postgres
postgres:
  host: localhost
  port: 5432
  database: flowbyte_destination
  credentials_ref: postgres_main_creds

resources:
  orders:
    enabled: true
    sync_mode: incremental        # incremental | full_refresh
    schedule: "0 */2 * * *"       # cron: mỗi 2h
    transform:
      rename:
        total_price: revenue
      skip:
        - note_attributes
      type_override:
        total_price: numeric(12,2)
  customers:
    enabled: true
    sync_mode: incremental
    schedule: "0 */2 * * *"
  inventory_levels:
    enabled: true
    sync_mode: full_refresh      # không có updated_at hiệu quả
    schedule: "0 */2 * * *"
  locations:
    enabled: true
    sync_mode: full_refresh
    schedule: "0 3 * * *"        # 3am hàng ngày

weekly_full_refresh:
  enabled: true
  schedule: "0 2 * * 0"          # Chủ nhật 2am
```

**Done khi:**
- ✅ 8 CLI commands hoạt động
- ✅ `validate` < 5 giây
- ✅ YAML Git-friendly, KHÔNG chứa credentials plaintext
- ✅ Credentials encrypted AES-256-GCM, master key `/etc/flowbyte/master.key` (chmod 600)
- ✅ `delete` không corrupt data khi sync đang chạy (graceful stop hoặc `--force` confirm)

**Edge cases:**
- Sai Haravan token → `validate` báo 401, gợi ý Haravan admin → Apps → API
- Sai Postgres creds → báo connection error, gợi ý check firewall/pg_hba.conf
- Trùng tên pipeline → báo lỗi
- Master key mất → CLI fail, hướng dẫn restore từ backup; credentials không decrypt được
- `delete` khi sync đang chạy → confirm `[y/N]`; yes thì đợi sync xong hoặc `--force` kill

---

#### F3: CLI Observability (`status`, `logs`, `history`)
**Story:** Là Data Analyst, tôi muốn xem nhanh trạng thái pipeline và log qua CLI để debug mà không cần Web UI.

**CLI commands:**
```
flowbyte status                            # overview tất cả pipelines
flowbyte history <pipeline> [--last 10]    # 10 lần sync gần nhất
flowbyte logs <pipeline> [--tail]          # xem log gần nhất
flowbyte logs <pipeline> --errors          # filter chỉ ERROR/CRITICAL
flowbyte logs <pipeline> --since 1h        # log 1 giờ gần nhất
```

**Output mẫu `flowbyte status`:**
```
PIPELINE     RESOURCE          LAST SYNC           STATUS   RECORDS  NEXT SYNC
shop_main    orders            2026-04-24 10:15    🟢 OK    42       2026-04-24 12:15
shop_main    customers         2026-04-24 10:15    🟢 OK    8        2026-04-24 12:15
shop_main    inventory_levels  2026-04-24 10:15    🔴 FAIL  0        2026-04-24 12:15
shop_main    products          2026-04-24 00:15    🟢 OK    125      2026-04-24 12:15

Scheduler: 🟢 Running (uptime 5d 3h)
Validation: ⚠️ 1 warning (inventory_levels)
```

**Done khi:**
- ✅ 5 CLI commands hoạt động
- ✅ `status` load < 1 giây
- ✅ 4 trạng thái rõ: 🟢 OK / 🔴 FAIL / 🟡 RUNNING / ⚪ DISABLED
- ✅ `history` hiển thị 10 lần sync gần nhất với time, records, status, error
- ✅ `logs --errors` filter đúng level ERROR/CRITICAL

**Edge cases:**
- Không có pipeline → gợi ý `"Run 'flowbyte init <name>' to create first pipeline"`
- Nhiều resource fail → gom hiển thị `"3 resources failed — run 'flowbyte logs <pipeline> --errors'"` thay vì spam
- Sync > 1h → hiển thị 🟡 + warning `"running 1h 23m — longer than expected"`
- Log file > 10GB → auto archive + retention cleanup (xem F8)

---

#### F4: Incremental Sync
**Story:** Là Data Analyst, tôi muốn chỉ sync records mới/updated kể từ lần cuối, để tiết kiệm quota Haravan và thời gian.

**Strategy:**
- **Cursor:** Timestamp-based với `?updated_at_min={checkpoint - 5min}&order=updated_at asc`
- **Checkpoint:** `MAX(updated_at)` của batch vừa sync (**high-watermark-from-batch**, không dùng `now()` → tránh clock drift)
- **Overlap 5 phút** để không miss record do delay Extract→Load; upsert idempotent nên overlap không gây duplicate
- **Atomicity:** Cập nhật checkpoint trong cùng transaction với Load data (rollback toàn bộ nếu fail)

**Sync mode theo resource (MVP):**

| Resource | Sync mode | Lý do |
|---|---|---|
| `orders` | Incremental 2h + Weekly Full Sun 2am | Có `updated_at`, volume cao |
| `customers` | Incremental 2h + Weekly Full | Có `updated_at` |
| `products` | Incremental 12h + Weekly Full | Có `updated_at`, ít đổi |
| `variants` | Sync cùng `products` (nested) | Không có endpoint riêng hiệu quả |
| `inventory_levels` | **Full Refresh 2h** | Endpoint phân trang hạn chế, volume thấp |
| `locations` | **Full Refresh 24h** | Ít thay đổi, volume rất thấp |

**Done khi:**
- ✅ Incremental nhanh hơn Full Refresh **≥ 10x** (76k full ≈ 76s; incremental 2h ≈ 1s với ~17 orders mới)
- ✅ Không duplicate (`SELECT count(*), count(DISTINCT id) FROM orders` bằng nhau)
- ✅ Records update trên Haravan được upsert đúng (không tạo bản ghi mới)
- ✅ Checkpoint atomic — fail giữa chừng không update checkpoint
- ✅ Lần sync đầu tiên (chưa có checkpoint) → tự động Full Refresh 1 lần

**Edge cases:**
- Resource không có `updated_at` → cấu hình `sync_mode: full_refresh`, không dùng checkpoint
- Record update với `updated_at` < checkpoint → bắt qua Weekly Full Refresh
- Record deleted trên Haravan → Weekly Full Refresh đánh dấu `_deleted_at = now()`
- Constraint vi phạm khi upsert → log record ID vào `sync_logs`, skip, tiếp tục
- Weekly Full Refresh đang chạy → sync incremental khác vẫn chạy, queue qua token bucket chung

---

#### F5: Data Validation (đơn giản hóa)
**Story:** Là Data Analyst, tôi muốn Flowbyte tự kiểm tra toàn vẹn cơ bản sau mỗi sync để phát hiện lỗi nghiêm trọng sớm.

**2 validation rules MVP:**
1. **Row count delta check:** So sánh row count trong Postgres trước/sau sync. Giảm > 10% (không phải Weekly Full Refresh) → flag warning.
2. **Required fields not null:** `id`, `created_at` của records vừa sync không null. Có null → flag error, log record ID.

**Bỏ khỏi MVP (phase 2+):** Count check với Haravan API (tốn quota), schema check, referential integrity.

**Done khi:**
- ✅ Validation tự chạy sau mỗi sync thành công
- ✅ Kết quả vào bảng `validation_results` internal Postgres
- ✅ Fail KHÔNG rollback data, chỉ flag + alert
- ✅ `flowbyte status` hiển thị validation: ✅ Healthy / ⚠️ Warning / ❌ Failed
- ✅ Validation < 5 giây/resource (chỉ SQL, không gọi Haravan)

**Edge cases:**
- Row count giảm do delete thật (refund/cancel) → chấp nhận ≤ 10%
- Weekly Full Refresh giảm > 10% → không flag (expected)
- `id` null (phòng edge case Haravan) → skip record, log warning

---

#### F6: Data Transformation (YAML field mapping)
**Story:** Là Data Analyst, tôi muốn đổi tên cột và skip cột không cần trong Postgres qua YAML, để schema destination gọn.

**MVP hỗ trợ:**
1. **Field rename:** `total_price` → `revenue`
2. **Field skip:** bỏ `note_attributes`
3. **Field type override:** ép kiểu `total_price` string → `numeric(12,2)`

**Khuyến nghị user:** Transformation phức tạp (computed, filter, join) làm tầng Postgres bằng **SQL view / dbt models** sau khi Load — dễ hơn, không break pipeline.

**Bỏ khỏi MVP (phase 3 NICE TO HAVE):** Computed field, filter rows, flatten custom, Python/SQL script, versioning, preview UI.

**Done khi:**
- ✅ YAML `transform` section parse được
- ✅ rename/skip/type_override hoạt động đúng
- ✅ Sai cú pháp → `flowbyte validate` báo lỗi, không save

**Edge cases:**
- Rename thành tên trùng cột khác → validate báo lỗi
- Type override sai kiểu → log lỗi từng record, skip record đó
- Skip cột `id` (PK) → validate báo lỗi, không cho phép

---

#### F7: Alerting qua Telegram
**Story:** Là Data Analyst, tôi muốn nhận alert Telegram ngay khi sync fail hoặc scheduler dead, để xử lý kịp thời mà không ngồi canh terminal.

**Setup:**
1. User tạo Telegram bot qua @BotFather → `bot_token`
2. User `/start` bot, lấy `chat_id` qua `https://api.telegram.org/bot{token}/getUpdates`
3. Config trong `/etc/flowbyte/config.yml`:
   ```yaml
   alerting:
     telegram:
       bot_token: <token>
       chat_id: <id>
   ```
4. Test: `flowbyte alert test`

**2 triggers bắt buộc MVP:**
- 🔴 **Critical:** Sync fail sau 3 lần retry
- 🔴 **Critical:** Scheduler không heartbeat > 2h (scheduler dead, heartbeat check mỗi 5 phút)

**Bỏ khỏi MVP (phase 2+):** Email, Slack, abnormal volume trigger, validation fail trigger, rate limit alert, multi-channel fallback.

**Done khi:**
- ✅ Alert gửi < 30 giây kể từ event
- ✅ `flowbyte alert test` gửi được message test
- ✅ Không spam: gom event trùng trong 5 phút thành 1 message
- ✅ Rate limit alert: max 3 alerts/giờ/pipeline

**Edge cases:**
- Telegram API fail → retry 3 lần, vẫn fail thì log local, không crash scheduler
- Sai `bot_token` → `alert test` báo lỗi rõ, không save
- VPS không có outbound tới `api.telegram.org` → gợi ý check firewall

---

#### F8: Log Management + Retention
**Story:** Là sysadmin, tôi muốn logs tự cleanup theo retention để không đầy disk VPS.

**Retention policy (hard-coded MVP):**

| Data | Retention | Lý do |
|---|---|---|
| `sync_logs` (success) | **90 ngày** | Audit + debug |
| `sync_logs` (error + stack trace) | **30 ngày** | Debug gần, cũ hơn không cần |
| `validation_results` | **30 ngày** | |
| `sync_checkpoints` | **Vĩnh viễn** | Cần cho incremental |
| Data tables (orders, customers...) | **Vĩnh viễn** | Data chính, user tự quản lý |

**Cleanup job:**
- Daily 3:00 AM (sau Weekly Full Refresh Chủ nhật không conflict)
- `DELETE FROM sync_logs WHERE created_at < now() - interval '90 days'`
- `VACUUM ANALYZE` các bảng sau delete
- Log kết quả vào `sync_logs` level INFO
- `flowbyte cleanup --dry-run` preview trước khi thực thi

**Done khi:**
- ✅ Cleanup chạy auto daily 3am, idempotent
- ✅ Sau 90 ngày, `sync_logs` không phình > 500 MB
- ✅ `--dry-run` flag hoạt động

---

### 🔵 SHOULD (Phase 2 — sau MVP)

#### F9: Web UI (Dashboard + Pipeline CRUD + Settings)
- Dashboard real-time (WebSocket auto-refresh 10s)
- Pipeline CRUD qua form UI
- Settings: credentials, alerting, retention config
- JWT + bcrypt auth, username/password login
- Cùng source of truth với CLI (đọc/ghi YAML + internal Postgres)

#### F10: Alerting mở rộng
- Thêm channels: Email SMTP, Slack webhook
- Thêm triggers: validation fail, abnormal volume (< 50% trung bình 7 ngày), sync > ngưỡng
- Fallback channel (Slack fail → Email)

#### F11: Phase 2 resources
- `refunds`, `fulfillments`, `transactions`, `order_risks`, `draft_orders`
- Doanh thu net, track giao hàng, thanh toán

---

### 🟠 NICE TO HAVE (Phase 3+)

- **F12: Transformation custom nâng cao** — computed field, filter rows, flatten custom, SQL expression sandbox, versioning + rollback, preview 10 records UI. Python script optional (risk bảo mật cao — cần sandbox Docker).
- **F13: Log UI** — search/filter, export CSV/JSON, mask sensitive.
- **F14: Phase 3/4 resources** — collections, discounts, coupons, gift_cards, users, blogs, pages, redirects, webhooks, carriers, checkouts.
- **F15: Multi-shop support** — 1 instance Flowbyte sync nhiều shop Haravan (queue per-shop).
- **F16: Data quality monitoring** — schema evolution alert, abnormal pattern detection.

---

## ⑤ Tech Stack

| Layer | Tech | Lý do |
|---|---|---|
| CLI | **Python 3.11+** với **Typer** (hoặc Click) | Ecosystem ETL mạnh, Data Analyst dễ đọc/sửa |
| Scheduler | **APScheduler** trong cùng process (daemon) | Không cần thêm service; recovery state qua jobstore Postgres |
| ETL core | **requests**, **SQLAlchemy**, **pydantic** | Không dùng pandas để tránh memory heavy |
| Destination DB | **PostgreSQL ≥ 14** | JSONB GIN index tốt từ PG14 |
| Internal DB | **PostgreSQL ≥ 14** (database riêng, cùng instance) | |
| Encryption | `cryptography` lib, **AES-256-GCM** | Chuẩn industry credentials at-rest |
| Hosting | **Docker Compose** trên VPS (dùng chung n8n) | Tận dụng VPS sẵn có, isolation qua Docker |
| Alerting | **Telegram Bot API** | Free, push mobile, setup 5 phút |
| Frontend (Phase 2) | Next.js (React) | Ecosystem mạnh |

**Docker Compose setup:**
- `flowbyte` container: memory limit **1.5 GB** (tránh OOM ảnh hưởng n8n)
- Shared Postgres instance (2 databases: `flowbyte_destination` + `flowbyte_internal`)
- Networks: cùng docker network với n8n nếu cần share data, hoặc bridge riêng

---

## ⑥ Integration Points

### 🔗 Haravan API (Extract)
- **Base URL:** `https://{shop}.myharavan.com/admin/{resource}.json`
- **Auth:** Header `X-Haravan-Access-Token: <token>` (Private app token từ Haravan admin)
- **Rate limit:** Leaky bucket **80 burst, 4 req/s leak** ([docs](https://docs.haravan.com/docs/omni-apis/api-call-limit/))
  - Client-side token bucket sync với response header `X-Haravan-Api-Call-Limit: current/80`
  - 429 → đọc `Retry-After` header, sleep, retry exponential backoff (max 5 lần)
- **Pagination:** Cursor-based (`?page_info=`) hoặc page-based (`?page=&limit=250`), tùy endpoint
- **Incremental filter:** `?updated_at_min={ISO8601}&order=updated_at asc`
- **Assumption:** Shop plan **Haravan Omnichannel** (API đầy đủ)

❌ Không xử lý: Webhook real-time, GraphQL API

---

### 🔗 PostgreSQL (Load + Internal)
- **2 databases** trên cùng instance:
  - `flowbyte_destination`: data Haravan (orders, customers, products...)
  - `flowbyte_internal`: metadata (credentials encrypted, pipelines, sync_logs, checkpoints, validation_results)
- Tự động `CREATE TABLE` lần đầu với **hybrid schema** (xem ⑦)
- Upsert: `INSERT ... ON CONFLICT (id) DO UPDATE SET ...` trong transaction atomic
- Cột `_raw JSONB` mọi bảng → handle field mới từ Haravan không breaking
- **KHÔNG auto ALTER TABLE** khi Haravan thêm field — field mới tự vào `_raw`
- Soft delete: cột `_deleted_at TIMESTAMP NULL`, Weekly Full Refresh đánh dấu record biến mất

❌ Không xử lý: Cross-database transaction, PostgreSQL < 14

---

### 🔗 Telegram Bot API (Alerting)
- User tạo bot qua **@BotFather** → `bot_token`
- User `/start` bot, lấy `chat_id` qua `getUpdates`
- Flowbyte `POST https://api.telegram.org/bot{token}/sendMessage` với `chat_id` + `text`
- Rate limit Telegram: 30 msg/s — không phải vấn đề với 2 triggers MVP

❌ Không xử lý: Email, Slack, Discord, SMS (Phase 2+)

---

## ⑦ Data Schema (Hybrid Strategy)

### Nguyên tắc: Flatten top-level + nested objects, JSONB cho arrays phức tạp + `_raw JSONB` cho future-proof

### 📊 Bảng flatten (MVP Phase 1):

```sql
-- orders: flatten top-level + customer/shipping_address thành cột
CREATE TABLE orders (
  id              BIGINT PRIMARY KEY,
  order_number    TEXT,
  total_price     NUMERIC(12,2),
  subtotal_price  NUMERIC(12,2),
  total_tax       NUMERIC(12,2),
  financial_status TEXT,
  fulfillment_status TEXT,
  -- flatten customer.*
  customer_id     BIGINT,
  customer_email  TEXT,
  customer_phone  TEXT,
  -- flatten shipping_address.*
  shipping_name   TEXT,
  shipping_phone  TEXT,
  shipping_address1 TEXT,
  shipping_city   TEXT,
  shipping_province TEXT,
  shipping_country TEXT,
  -- nested arrays → JSONB
  transactions    JSONB,
  tax_lines       JSONB,
  discount_codes  JSONB,
  fulfillments    JSONB,
  note_attributes JSONB,
  -- metadata
  created_at      TIMESTAMPTZ NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL,
  _raw            JSONB NOT NULL,
  _deleted_at     TIMESTAMPTZ NULL,
  _synced_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_orders_updated_at ON orders (updated_at);
CREATE INDEX idx_orders_active ON orders (id) WHERE _deleted_at IS NULL;

-- order_line_items: 1 line = 1 row (flatten cho báo cáo doanh thu theo SP)
CREATE TABLE order_line_items (
  id              BIGINT PRIMARY KEY,
  order_id        BIGINT NOT NULL REFERENCES orders(id),
  product_id      BIGINT,
  variant_id      BIGINT,
  sku             TEXT,
  title           TEXT,
  quantity        INTEGER,
  price           NUMERIC(12,2),
  _raw            JSONB NOT NULL,
  _synced_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_line_items_order ON order_line_items (order_id);
CREATE INDEX idx_line_items_product ON order_line_items (product_id);

-- customers
CREATE TABLE customers (
  id              BIGINT PRIMARY KEY,
  email           TEXT,
  phone           TEXT,
  first_name      TEXT,
  last_name       TEXT,
  orders_count    INTEGER,
  total_spent     NUMERIC(12,2),
  tags            TEXT,
  created_at      TIMESTAMPTZ,
  updated_at      TIMESTAMPTZ NOT NULL,
  _raw            JSONB NOT NULL,
  _deleted_at     TIMESTAMPTZ NULL,
  _synced_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- products
CREATE TABLE products (
  id              BIGINT PRIMARY KEY,
  title           TEXT,
  vendor          TEXT,
  product_type    TEXT,
  handle          TEXT,
  status          TEXT,
  tags            TEXT,
  created_at      TIMESTAMPTZ,
  updated_at      TIMESTAMPTZ NOT NULL,
  _raw            JSONB NOT NULL,
  _deleted_at     TIMESTAMPTZ NULL,
  _synced_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- variants (nested trong products, sync cùng)
CREATE TABLE variants (
  id                  BIGINT PRIMARY KEY,
  product_id          BIGINT NOT NULL REFERENCES products(id),
  sku                 TEXT,
  title               TEXT,
  price               NUMERIC(12,2),
  compare_at_price    NUMERIC(12,2),
  inventory_item_id   BIGINT,
  inventory_quantity  INTEGER,
  updated_at          TIMESTAMPTZ NOT NULL,
  _raw                JSONB NOT NULL,
  _synced_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- inventory_levels (Full Refresh)
CREATE TABLE inventory_levels (
  inventory_item_id   BIGINT NOT NULL,
  location_id         BIGINT NOT NULL,
  available           INTEGER,
  updated_at          TIMESTAMPTZ,
  _raw                JSONB NOT NULL,
  _synced_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (inventory_item_id, location_id)
);

-- locations (Full Refresh)
CREATE TABLE locations (
  id              BIGINT PRIMARY KEY,
  name            TEXT,
  address1        TEXT,
  city            TEXT,
  province        TEXT,
  country         TEXT,
  active          BOOLEAN,
  _raw            JSONB NOT NULL,
  _deleted_at     TIMESTAMPTZ NULL,
  _synced_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 📊 Internal Postgres tables:

```sql
CREATE TABLE credentials (
  ref             TEXT PRIMARY KEY,
  encrypted_blob  BYTEA NOT NULL,       -- AES-256-GCM ciphertext
  nonce           BYTEA NOT NULL,
  created_at      TIMESTAMPTZ DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE pipelines (
  name            TEXT PRIMARY KEY,
  enabled         BOOLEAN DEFAULT true,
  yaml_path       TEXT NOT NULL,
  created_at      TIMESTAMPTZ DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE sync_checkpoints (
  pipeline_name   TEXT NOT NULL,
  resource        TEXT NOT NULL,
  checkpoint_ts   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (pipeline_name, resource)
);

CREATE TABLE sync_logs (
  id              BIGSERIAL PRIMARY KEY,
  pipeline_name   TEXT NOT NULL,
  resource        TEXT NOT NULL,
  sync_mode       TEXT NOT NULL,        -- 'incremental' | 'full_refresh'
  status          TEXT NOT NULL,        -- 'success' | 'failed' | 'running'
  records_synced  INTEGER,
  duration_ms     INTEGER,
  error_message   TEXT,
  error_stack     TEXT,
  started_at      TIMESTAMPTZ NOT NULL,
  finished_at     TIMESTAMPTZ,
  created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_sync_logs_pipeline ON sync_logs (pipeline_name, created_at DESC);

CREATE TABLE validation_results (
  id              BIGSERIAL PRIMARY KEY,
  pipeline_name   TEXT NOT NULL,
  resource        TEXT NOT NULL,
  rule            TEXT NOT NULL,
  status          TEXT NOT NULL,        -- 'passed' | 'warning' | 'failed'
  details         JSONB,
  created_at      TIMESTAMPTZ DEFAULT now()
);
```

### 📊 JSONB columns trong `orders`:
- `transactions` — payment transactions (ít query trực tiếp)
- `tax_lines` — chi tiết thuế (nếu cần báo cáo VAT thì extract)
- `discount_codes` — mã khuyến mãi
- `fulfillments` — trạng thái giao hàng
- `note_attributes` — ghi chú custom

**Query JSONB khi cần** (cho analyst):
```sql
-- Tổng tiền transactions theo gateway
SELECT
  (txn->>'gateway') AS gateway,
  SUM((txn->>'amount')::numeric) AS total
FROM orders, jsonb_array_elements(transactions) txn
GROUP BY 1;
```

### 📊 Schema evolution strategy:
- Haravan thêm field mới → field tự động vào `_raw JSONB`, không break
- **KHÔNG auto ALTER TABLE ADD COLUMN** (tránh type infer sai)
- User tự `ALTER TABLE orders ADD COLUMN new_field TEXT GENERATED ALWAYS AS (_raw->>'new_field') STORED` khi muốn promote field mới lên thành cột chính

---

## ⑧ Non-Functional Requirements

### Performance
- CLI command response < 1s (except `run`, `validate`)
- `validate` (test connection) < 5s
- Sync **10k records < 1 phút** (rate limit 4 req/s × 250 records/page)
- Full refresh 76k orders (shop hiện tại) **< 3 phút**
- Validation < 5s/resource

### Security
- Credentials **AES-256-GCM** encrypted trong `flowbyte_internal.credentials`
- Master key `/etc/flowbyte/master.key`, **chmod 600**, user tự backup
- YAML config Git-friendly, KHÔNG chứa credentials plaintext
- Mask `access_token`/`password` trong logs (chỉ hiển thị 4 ký tự cuối, vd `****abcd`)
- **Linux file permission** thay cho JWT/bcrypt auth trong MVP (CLI chạy qua SSH đã auth)
- HTTPS only khi gọi Haravan + Telegram

### Reliability
- Scheduler uptime ≥ 99% (đo bằng `sync_logs` success_rate + systemd uptime)
- Checkpoint atomic — sync fail không corrupt checkpoint
- VPS restart → systemd auto restart Flowbyte, scheduler khôi phục

### Resource (dùng chung VPS với n8n)
- Memory container Flowbyte: **limit 1.5 GB** (VPS 4GB: 1.5 Flowbyte + 1.5 n8n + 1 OS)
- Disk: ước tính **10 GB MVP, 30 GB sau 3 năm** → VPS ≥ 50 GB total (user đang 27 GB, sẽ upgrade)
- Postgres connections pool: 10 (đủ cho 1 scheduler + CLI ad-hoc)

### Concurrency (MVP)
- **1 shop, 1 pipeline** — không multi-shop concurrency
- Tất cả resources queue tuần tự qua **1 token bucket Haravan chung**
- Pipeline-level lock: 2 sync trùng 1 resource → sync sau đợi

### Uptime target
- 99% (cho phép downtime maintenance ~7 giờ/tháng)

---

## ⑨ Edge Cases & Error States

| Tình huống | Hành vi mong muốn |
|---|---|
| User nhập sai Haravan credentials | `validate` báo 401, không save pipeline |
| User nhập sai Postgres credentials | `validate` báo connection error, gợi ý check firewall/pg_hba |
| Mất mạng giữa sync | Rollback transaction, log error, retry lần sau từ checkpoint cũ |
| Disk VPS < 1 GB free | Dừng sync mới, alert critical Telegram |
| Haravan token hết hạn / revoked | 401 → alert critical, dừng sync, user `flowbyte creds set` refresh |
| Haravan 429 rate limit | Đọc `Retry-After`, exponential backoff, max 5 lần retry |
| Haravan 5xx server error | Retry 3 lần backoff, fail → log + alert |
| Postgres connection timeout | Retry 3 lần backoff, fail → pause pipeline + alert |
| Haravan thêm field mới | Field tự vào `_raw JSONB`, không crash, không auto ALTER |
| Haravan xóa field cũ | Cột cũ giữ nguyên NULL, không crash |
| VPS restart | systemd restart Flowbyte, scheduler khôi phục lịch đến hạn |
| Hai sync trùng 1 resource | Pipeline-level lock, sync sau đợi sync trước xong |
| User `flowbyte delete` khi sync đang chạy | Confirm `[y/N]`, yes thì đợi sync xong; `--force` để kill |
| Master key mất | CLI fail, hướng dẫn restore từ backup; credentials không decrypt được |
| Token bucket > 70/80 | Slow down throttle thêm tránh 429 |
| `sync_logs` > 10 GB | Cleanup job retention policy (90d/30d) |
| Clock drift VPS vs Haravan | High-watermark-from-batch + overlap 5 phút, không dùng `now()` VPS |
| n8n ăn hết RAM | Docker memory limit 1.5 GB cho Flowbyte container → bảo vệ process |

---

## ⑩ Success Metrics

### Tuần 1 (sau deploy MVP)
- ✅ `flowbyte status` hiển thị Phase 1 (6 resources) sync **thành công ≥ 90%** (9/10 lần gần nhất)
- ✅ `orders` trong Postgres khớp count Haravan **±0.1%** sau full refresh đầu (verify `/orders/count.json` vs `SELECT COUNT(*)`)
- ✅ Telegram test message nhận được **< 30 giây**
- ✅ Full refresh 76k orders đầu tiên **< 3 phút**

### Tháng 1
- ✅ Sync success rate **≥ 99%** (`sync_logs`: success_count / total_count)
- ✅ Scheduler **không crash 30 ngày** (`systemctl status flowbyte` uptime ≥ 99%)
- ✅ Tạo **5 artifact** (SQL view / Metabase dashboard / Google Sheet / Superset) thay thế file Excel cũ:
  - `revenue_daily` — doanh thu theo ngày
  - `inventory_low_stock` — SKU tồn kho dưới ngưỡng
  - `customer_ltv` — customer lifetime value
  - `product_sales_rank` — top SKU bán chạy
  - `kpi_staff_monthly` — KPI nhân viên
- ✅ Chi phí VPS **≤ $9/tháng** (= 30% Airbyte cũ $30) — share VPS với n8n, không tăng cost
- ✅ **0 lần login Haravan Admin export CSV** (tự theo dõi trong 30 ngày)

### Tháng 3
- ✅ 100% báo cáo BI pull từ Postgres (không còn Haravan CSV)
- ✅ Phase 2 resources (`refunds`, `fulfillments`, `transactions`) sync ổn định ≥ 7 ngày
- ✅ Pipeline uptime ≥ 99% đo bằng `sync_logs`
- ✅ ≥ 1 transformation YAML field mapping chạy production
- ✅ ≥ 2 đội khác consume data qua Postgres (SQL view / BI tool) — không tự tạo pipeline

---

## ⑪ Constraints & Assumptions

### 🚧 Giới hạn

| Loại | Giới hạn |
|---|---|
| **Budget** | Dùng VPS có sẵn; target ≤ $9/tháng (30% Airbyte cũ $30) |
| **Timeline** | Không deadline cố định; phase: MVP CLI → Web UI → Phase 2/3/4 resources |
| **Team** | Solo khởi đầu, linh hoạt scale |
| **Tech** | VPS Linux (Ubuntu/Debian); Docker Compose; share với n8n, memory limit 1.5GB; Postgres ≥ 14; không phụ thuộc cloud trả phí |

### ✅ Assumptions (verify trước khi implement)

| # | Assumption | Verify | Status |
|---|---|---|---|
| 1 | Shop dùng plan **Haravan Omnichannel** | User self-confirm | ✅ |
| 2 | Haravan API ổn định, không breaking 6 tháng | Monitor release notes; `_raw JSONB` resilient | ⏳ ongoing |
| 3 | User có quyền admin Haravan tạo private app → access token | Haravan admin → Apps → Private apps | ⚠️ verify |
| 4 | VPS có outbound HTTPS tới `*.haravan.com` + `api.telegram.org` | `curl -I https://...` test trước cài | ⚠️ verify |
| 5 | VPS có đủ disk ≥ 50 GB | Hiện 27 GB, user upgrade | ⚠️ pending upgrade |
| 6 | PostgreSQL ≥ 14 có sẵn hoặc cài mới | `psql --version`; `apt install postgresql-14` nếu chưa | ⚠️ verify |
| 7 | User tự backup Postgres (pg_dump cronjob) | User self-confirm; Flowbyte KHÔNG tự backup MVP | ✅ |
| 8 | Data volume SME: < 1M records/resource | 76k orders hiện tại, 10 năm ≈ 800k — OK | ✅ |
| 9 | User nội bộ MVP = 1 (Data Analyst) | Linux permission đủ; multi-user Phase 2 | ✅ |
| 10 | User quen SQL + terminal + cron | Persona confirmed | ✅ |
| 11 | Shop: 1 Haravan duy nhất | User confirmed | ✅ |
| 12 | Timezone: UTC internal, convert khi display | Postgres `timestamptz`, Haravan trả ISO 8601 có tz | ✅ |
| 13 | n8n không ăn hết RAM (cạnh tranh Flowbyte) | Docker memory limit 1.5GB isolation | ✅ |

---

## ⑫ Deployment Checklist (MVP)

### Pre-install verify
- [ ] Haravan Omnichannel plan confirmed
- [ ] Access token tạo sẵn (Haravan admin → Apps → Private apps)
- [ ] VPS outbound firewall cho `*.haravan.com` + `api.telegram.org`
- [ ] VPS disk ≥ 50 GB (upgrade từ 27 GB)
- [ ] PostgreSQL ≥ 14 installed
- [ ] Docker + Docker Compose installed
- [ ] n8n memory usage kiểm tra (còn room cho Flowbyte 1.5 GB)
- [ ] Telegram bot tạo qua @BotFather, `bot_token` + `chat_id` lấy sẵn

### Install steps
1. `git clone` Flowbyte repo vào `/opt/flowbyte`
2. Generate master key: `flowbyte init-master-key` → ghi vào `/etc/flowbyte/master.key`, chmod 600, **backup ra ngoài VPS**
3. Tạo 2 databases: `flowbyte_destination` + `flowbyte_internal`
4. Run migrations: `flowbyte migrate`
5. Cấu hình `/etc/flowbyte/config.yml` (Telegram alerting)
6. `flowbyte init shop_main` → edit YAML
7. `flowbyte creds set shop_main_creds` → nhập Haravan access_token
8. `flowbyte validate shop_main`
9. `flowbyte run shop_main` (full refresh đầu tiên, ~3 phút)
10. `flowbyte enable shop_main` → scheduler tự chạy
11. `flowbyte alert test` → confirm Telegram nhận được
12. Setup `pg_dump` cronjob backup (ngoài Flowbyte, user tự làm)

### Post-install verify
- [ ] `flowbyte status` hiển thị 6 resources Phase 1 🟢 OK
- [ ] Weekly Full Refresh scheduled Chủ nhật 2am
- [ ] `flowbyte logs shop_main --tail` không có ERROR
- [ ] Sample query: `SELECT COUNT(*) FROM orders;` khớp Haravan ±0.1%
- [ ] Telegram test message đã nhận < 30s

---

## ⑬ User Stories (Backlog)

> Đánh số từ PRD features, sắp xếp theo thứ tự implement hợp lý — story có dependency phải làm sau story được phụ thuộc.
>
> **Status:** 📝 Draft | ⏳ Todo | 🔄 In Progress | ⚠️ Partial | ✅ Done
>
> **Quy trình mỗi buổi build:** Mở file này → chọn story `⏳ Todo` tiếp theo → đổi sang `🔄 In Progress` → implement → test → `✅ Done`

| ID | Tên story | Mô tả ngắn | Phức tạp | Phụ thuộc vào | Sprint | Status |
|---|---|---|---|---|---|---|
| **US-001** | F2: Quản lý Pipeline qua CLI + YAML | 8 CLI commands, YAML config, credentials mã hóa AES-256-GCM, master key | M | — | S1 | ⚠️ Partial |
| **US-002** | F4: Incremental Sync | Cursor timestamp, checkpoint atomic, overlap 5 phút, tự Full Refresh lần đầu | M | US-001 | S1 | ⚠️ Partial |
| **US-003** | F6: Data Transformation YAML | rename / skip / type_override field qua YAML config, validate trước save | S | US-001 | S1 | ⚠️ Partial |
| **US-004** | F1: Core Sync Engine | ETL pipeline đầy đủ: 6 resources Phase 1, scheduler APScheduler, token bucket rate limit | L | US-001, US-002, US-003 | S1 | ⚠️ Partial |
| **US-005** | F7: Alerting Telegram | Alert sync fail + scheduler dead, không spam, `flowbyte alert test` | S | US-004 | S2 | ⚠️ Partial |
| **US-006** | F5: Data Validation | 2 rules: row count delta ≤10% + required fields not null, lưu `validation_results` | S | US-004 | S2 | ⚠️ Partial |
| **US-007** | F3: CLI Observability | `status` / `history` / `logs` commands, 4 trạng thái, filter `--errors` / `--tail` / `--since` | S | US-004 | S2 | ⚠️ Partial |
| **US-008** | F8: Log Retention | Cleanup daily 3am, retention 90d/30d, `VACUUM ANALYZE`, `--dry-run` | S | US-004, US-007 | S3 | ⚠️ Partial |
| **US-009** | F9: Web UI Dashboard | Real-time dashboard WebSocket 10s, pipeline CRUD form, JWT auth, Next.js | L | US-004, US-007 | — | ⏳ Todo |
| **US-010** | F10: Extended Alerting | Thêm Email SMTP / Slack webhook, triggers validation fail + abnormal volume, fallback channel | M | US-005 | — | ⏳ Todo |
| **US-011** | F11: Phase 2 Resources | 5 resources: `refunds`, `fulfillments`, `transactions`, `order_risks`, `draft_orders` | M | US-004, US-002 | — | ⏳ Todo |
| **US-012** | F12: Advanced Transformation | Computed field, filter rows, SQL expression sandbox, version/rollback, preview 10 records | L | US-003, US-009 | — | ⏳ Todo |
| **US-013** | F13: Log UI | Search/filter log trong Web UI, export CSV/JSON, mask sensitive data | M | US-007, US-009 | — | ⏳ Todo |
| **US-014** | F14: Phase 3/4 Resources | 11 resources: `collections`, `discounts`, `gift_cards`, `users`, `blogs`, `webhooks`... | M | US-011 | — | ⏳ Todo |
| **US-015** | F15: Multi-shop | 1 Flowbyte instance sync nhiều shop Haravan, queue per-shop token bucket | L | US-001, US-004 | — | ⏳ Todo |
| **US-016** | F16: Data Quality Monitoring | Schema evolution alert, abnormal pattern detection | M | US-006 | — | ⏳ Todo |

> **⚠️ Partial (US-001 → US-008):** code structure + unit tests xanh, chưa test end-to-end với Haravan API thật và Postgres thật.
> **— (US-009+):** Phase 2 trở đi, nằm ngoài 3 sprint MVP.

---

### Sprint Plan (MVP CLI)

#### Sprint 1 — "App chạy được"
**Milestone:** `flowbyte validate shop_main` pass + `flowbyte run shop_main --resource orders` sync được records thật vào Postgres.

| Story | Lý do |
|---|---|
| US-001 | Foundation bắt buộc — không có CLI/YAML/credentials thì không tạo được pipeline |
| US-002 | Checkpoint logic — thiếu thì mỗi sync đều full refresh, tốn hết quota Haravan |
| US-003 | Transform step trong pipeline — thiếu thì runner crash khi parse config transform |
| US-004 | Story tích hợp — gọi US-001/002/003, đây là lúc ETL thực sự chạy end-to-end |

#### Sprint 2 — "Demo được"
**Milestone:** Demo 5 phút: status 6 resources xanh → kill Postgres → nhận Telegram alert trong 30s → restart → xanh lại.

| Story | Lý do |
|---|---|
| US-007 | `flowbyte status` là "màn hình" của user — không có thì không biết gì đang chạy |
| US-005 | Alert Telegram = proof of value rõ nhất: *không cần ngồi canh terminal* |
| US-006 | Row count validation = chứng minh data đúng bằng số, tăng trust khi demo |

#### Sprint 3 — "Production-ready"
**Milestone:** Deploy Docker Compose lên VPS, `systemctl status flowbyte` uptime ≥ 7 ngày không can thiệp.

| Story | Lý do |
|---|---|
| US-008 | Log retention phải có trước khi scheduler chạy liên tục — không cleanup thì vài tuần đầy disk |
| Edge cases (từ bảng ⑨) | Rate limit 429, VPS restart recovery, disk < 1GB, Haravan 5xx — tất cả cần hoàn thiện trước production |
| Docker Compose + systemd | Deployment artifact thật — không có thì chỉ chạy local được |
| E2E verify | `SELECT COUNT(*) FROM orders` khớp `/orders/count.json` Haravan ±0.1% |

---

## Changelog

| Ngày | Version | Thay đổi | Người cập nhật |
|---|---|---|---|
| 2026-04-23 | v1 | Draft đầu tiên | Duy |
| 2026-04-24 | **v5** | Thêm cột Sprint (S1/S2/S3) vào bảng User Stories + Sprint Plan với milestone và lý do gom story. | Duy + Claude |
| 2026-04-24 | **v4** | Thêm cột Status vào bảng User Stories (📝⏳🔄⚠️✅), hướng dẫn quy trình mỗi buổi build, mark US-001→US-008 là ⚠️ Partial (Sprint 1 xong). | Duy + Claude |
| 2026-04-24 | **v3** | Thêm section ⑬ User Stories Backlog: đánh số US-001→US-016, bảng phức tạp S/M/L, dependency map, thứ tự implement hợp lý. | Duy + Claude |
| 2026-04-24 | **v2** | **PM review + chốt 9 vấn đề:** (1) MVP scope CLI-only, (2) Resources chia 4 phase, (3) Re-scope MUST features (F5 đơn giản, F6 field mapping YAML, F7 Telegram nâng MUST, F3 CLI commands), (4) Rate limit Haravan leaky bucket 80/4rps, concurrency 1 bucket/shop, (5) Config hybrid YAML + Postgres encrypted AES-256-GCM, bỏ JWT/bcrypt MVP, (6) Schema hybrid flatten + JSONB + `_raw`, deletion Weekly Full Refresh + soft delete, (7) Incremental timestamp + high-watermark-from-batch + overlap 5 phút, (8) Alerting Telegram nâng MUST + CLI observability + retention 90d/30d, (9) Success metrics đo được, Airbyte baseline $30 → target $9, Omnichannel plan, VPS share n8n Docker isolation. Thêm data flow diagram, YAML config example, SQL schema chi tiết, deployment checklist. | Duy + Claude PM review |
