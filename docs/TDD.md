# TDD — Flowbyte ETL Tool

**Version:** 1.0  
**Ngày:** 2026-04-24  
**Author:** Duy Ngo

---

## §1. Kiến trúc tổng thể

```
┌──────────────┐     HTTPS      ┌───────────────────┐
│  Haravan API │ ◄────────────► │  HaravanClient    │
│  (REST v1)   │                │  + TokenBucket    │
└──────────────┘                └────────┬──────────┘
                                         │
                              ┌──────────▼──────────┐
                              │    SyncRunner        │
                              │  extract → transform │
                              │  → validate → load   │
                              └──────────┬──────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                           │
   ┌──────────▼──────────┐   ┌──────────▼──────────┐   ┌──────────▼──────────┐
   │  Internal DB         │   │  Destination DB      │   │  Logging / Alerting  │
   │  (control plane)     │   │  (data plane)        │   │  structlog + Telegram│
   │  PostgreSQL          │   │  PostgreSQL           │   └──────────────────────┘
   └──────────────────────┘   └──────────────────────┘
```

**Process model:** Single Python process, APScheduler với `max_workers=1`, chạy như systemd service.

**Two-database design:**
- **Internal DB** (`flowbyte_internal`): control plane — pipelines, credentials, sync state, logs
- **Destination DB** (user-configured): data plane — business tables (orders, customers, ...)

---

## §2. Cấu trúc thư mục

```
src/flowbyte/
├── cli/              # Typer CLI commands
│   └── commands/     # pipeline, sync, system, observability, alerting
├── config/           # Pydantic models + YAML loader
├── db/               # SQLAlchemy table defs + engine factory
├── haravan/          # API client, token bucket, exceptions, per-resource extractors
├── sync/             # Transform, Load, Runner, Checkpoint
├── scheduler/        # APScheduler daemon + reconciler
├── security/         # AES-256-GCM encryption + master key
├── logging/          # structlog config, DB sink, event names, processors
├── validation/       # Post-sync validation rules
├── alerting/         # Telegram sender + deduper
├── observability/    # Metrics queries
├── retention/        # Cleanup jobs
└── bootstrap/        # First-time setup
```

---

## §3. Configuration

### §3.1 AppSettings (env vars / .env)

| Var | Default | Mô tả |
|---|---|---|
| `FLOWBYTE_DB_URL` | (required) | PostgreSQL URL của internal DB |
| `FLOWBYTE_PIPELINES_DIR` | `/etc/flowbyte/pipelines` | Thư mục chứa pipeline YAML files |
| `FLOWBYTE_MASTER_KEY_PATH` | `/etc/flowbyte/master.key` | Path tới master key |
| `FLOWBYTE_LOG_LEVEL` | `INFO` | Log level |

### §3.2 Pipeline YAML structure

```yaml
name: my_shop
haravan_credentials_ref: haravan_my_shop
haravan_shop_domain: myshop.myharavan.com

destination:
  host: localhost
  port: 5432
  user: flowbyte
  database: shop_data
  credentials_ref: pg_shop_data

resources:
  orders:
    enabled: true
    sync_mode: incremental      # incremental | full_refresh
    schedule: "0 */2 * * *"
    weekly_full_refresh:
      enabled: true
      cron: "0 3 * * 0"
    transform:
      rename:
        email: customer_email_alias
      skip: []
      type_override:
        total_price: "numeric(12,2)"
```

### §3.3 Global config.yml

```yaml
logging:
  db_sink:
    enabled: true
    min_level: INFO
    retention_days: 14
    max_payload_bytes: 10240
    on_sink_failure: drop
    queue_size: 1000
  batch_events_every_n: 10

alerting:
  telegram:
    enabled: false
    bot_token: ""
    chat_id: ""

validation:
  fetch_upsert_parity_max_skip_pct: 1.0
  volume_sanity_zero_streak: 3
  weekly_full_drift_warn_pct: 5.0
  weekly_full_drift_fail_pct: 20.0
  soft_delete_sanity_max_pct: 5.0
```

---

## §4. Security

### §4.1 Master Key

- Tạo bằng `os.urandom(32)` (256-bit)
- Lưu tại `FLOWBYTE_MASTER_KEY_PATH` (mặc định `/etc/flowbyte/master.key`)
- Permissions: `chmod 600` — chỉ owner đọc được
- Bootstrap kiểm tra file đã tồn tại trước khi tạo mới (không overwrite)
- Fingerprint (SHA-256) lưu vào bảng `master_key_metadata` để verify

### §4.2 Credential Encryption

**Algorithm:** AES-256-GCM

```
encrypt(plaintext, key, aad=ref):
    nonce = os.urandom(12)          # 96-bit nonce
    ciphertext, tag = AES-GCM(key, nonce, plaintext, aad)
    return base64(nonce || tag || ciphertext)

decrypt(ciphertext_b64, key, aad=ref):
    nonce, tag, ct = base64.decode(ciphertext_b64).split()
    return AES-GCM-decrypt(key, nonce, ct, tag, aad)
```

- **AAD binding:** `ref` (credential reference name) được dùng làm associated data. Nếu attacker copy ciphertext sang ref khác, decrypt thất bại (authentication error).
- **Serialization format:** `base64(nonce[12] || tag[16] || ciphertext[N])` lưu vào cột `text`

### §4.3 Credential Flow

```
flowbyte creds set haravan_shop → encrypt(plaintext, master_key, aad="haravan_shop")
                                 → store in credentials.ciphertext
                                 
reconciler._load_credentials(ref) → load ciphertext from DB
                                  → decrypt(ciphertext, master_key, aad=ref)
                                  → return dict {"access_token": "...", "shop_domain": "..."}
```

### §4.4 DB Connection URL Security

Postgres password được percent-encode (`urllib.parse.quote(password, safe="")`) trước khi embed vào connection URL để tránh parsing lỗi với ký tự đặc biệt (`@`, `:`, `/`).

---

## §5. Internal DB Schema

### §5.1 Bảng `pipelines`

```sql
CREATE TABLE pipelines (
    name         VARCHAR(32) PRIMARY KEY,
    yaml_content TEXT        NOT NULL,
    config_json  JSONB       NOT NULL,
    enabled      BOOLEAN     NOT NULL DEFAULT false,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### §5.2 Bảng `credentials`

```sql
CREATE TABLE credentials (
    ref        VARCHAR(64) PRIMARY KEY,
    kind       VARCHAR(32) NOT NULL CHECK (kind IN ('haravan', 'postgres')),
    ciphertext TEXT        NOT NULL,  -- base64(nonce||tag||ct)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### §5.3 Bảng `sync_requests`

```sql
CREATE TABLE sync_requests (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline         VARCHAR(32) NOT NULL,
    resource         VARCHAR(32),  -- NULL = all resources
    mode             VARCHAR(16)  NOT NULL DEFAULT 'incremental'
                                  CHECK (mode IN ('incremental', 'full_refresh')),
    status           VARCHAR(16)  NOT NULL DEFAULT 'pending'
                                  CHECK (status IN ('pending','claimed','running','done','failed','cancelled')),
    requested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    error            TEXT,
    recovery_count   SMALLINT    NOT NULL DEFAULT 0,
    wait_timeout_at  TIMESTAMPTZ
);

CREATE INDEX idx_sync_requests_pending
    ON sync_requests (status, requested_at)
    WHERE status = 'pending';
```

### §5.4 Bảng `sync_logs`

```sql
CREATE TABLE sync_logs (
    id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    level     VARCHAR(16)  NOT NULL CHECK (level IN ('DEBUG','INFO','WARNING','ERROR','CRITICAL')),
    event     VARCHAR(64)  NOT NULL,
    sync_id   UUID,
    pipeline  VARCHAR(32),
    resource  VARCHAR(32),
    message   TEXT,
    payload   JSONB,
    exc_info  TEXT
);

CREATE INDEX idx_sync_logs_pipeline_ts ON sync_logs (pipeline, resource, timestamp);
CREATE INDEX idx_sync_logs_sync_id     ON sync_logs (sync_id);
CREATE INDEX idx_sync_logs_errors      ON sync_logs (level, timestamp)
    WHERE level IN ('ERROR', 'CRITICAL');
```

### §5.5 Bảng `sync_checkpoints`

```sql
CREATE TABLE sync_checkpoints (
    pipeline        VARCHAR(32) NOT NULL,
    resource        VARCHAR(32) NOT NULL,
    last_updated_at TIMESTAMPTZ,
    last_id         BIGINT,      -- composite cursor tie-breaker
    last_sync_id    UUID,
    last_sync_at    TIMESTAMPTZ,
    last_status     VARCHAR(16),
    PRIMARY KEY (pipeline, resource)
);
```

> **§5.5 Note:** `last_id` là `BIGINT` (không phải `INTEGER`) vì Haravan IDs có thể vượt INT32_MAX trong một số trường hợp.

### §5.6 Bảng `sync_runs`

```sql
CREATE TABLE sync_runs (
    sync_id           UUID        PRIMARY KEY,
    pipeline          VARCHAR(32) NOT NULL,
    resource          VARCHAR(32) NOT NULL,
    mode              VARCHAR(16) NOT NULL,
    trigger           VARCHAR(16) NOT NULL,  -- schedule | manual
    request_id        UUID,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ,
    status            VARCHAR(16) NOT NULL DEFAULT 'running',
    fetched_count     INTEGER,
    upserted_count    INTEGER,
    skipped_invalid   INTEGER,
    soft_deleted_count INTEGER,
    rows_before       INTEGER,
    rows_after        INTEGER,
    duration_seconds  NUMERIC(10,2),
    error             TEXT
);

CREATE INDEX idx_sync_runs_recent  ON sync_runs (pipeline, resource, started_at);
CREATE INDEX idx_sync_runs_status  ON sync_runs (status, started_at)
    WHERE status IN ('failed', 'cancelled');
```

### §5.7 Bảng `validation_results`

```sql
CREATE TABLE validation_results (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    sync_id    UUID        NOT NULL,
    pipeline   VARCHAR(32) NOT NULL,
    resource   VARCHAR(32) NOT NULL,
    rule       VARCHAR(64) NOT NULL,
    status     VARCHAR(16) NOT NULL CHECK (status IN ('ok','warning','failed','skipped')),
    details    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_validation_sync ON validation_results (sync_id);
```

### §5.8 Bảng `scheduler_heartbeat` (singleton)

```sql
CREATE TABLE scheduler_heartbeat (
    id               INTEGER     PRIMARY KEY CHECK (id = 1),
    last_beat        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    daemon_started_at TIMESTAMPTZ NOT NULL,
    version          VARCHAR(32) NOT NULL
);
```

### §5.9 Materialized View `sync_rollup_daily`

```sql
CREATE MATERIALIZED VIEW sync_rollup_daily AS
SELECT
    DATE_TRUNC('day', finished_at AT TIME ZONE 'Asia/Ho_Chi_Minh') AS day,
    pipeline,
    resource,
    mode,
    COUNT(*) FILTER (WHERE status = 'success') AS success_count,
    COUNT(*) FILTER (WHERE status = 'failed')  AS failure_count,
    AVG(duration_seconds)                       AS avg_duration_s,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_seconds) AS p95_duration_s,
    SUM(fetched_count)                          AS total_fetched,
    SUM(upserted_count)                         AS total_upserted
FROM sync_runs
WHERE finished_at IS NOT NULL AND trigger = 'schedule'
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE UNIQUE INDEX idx_sync_rollup_daily_pk
    ON sync_rollup_daily (day, pipeline, resource, mode);
```

> **§5.9 Note:** Timezone `Asia/Ho_Chi_Minh` để rollup theo ngày Việt Nam, không phải UTC.

---

## §6. Destination DB Schema

### §6.1 Meta columns

Mọi table destination đều có:

```sql
_raw        JSONB       NOT NULL,      -- JSON gốc từ Haravan API
_synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
_sync_id    UUID        NOT NULL,
_deleted_at TIMESTAMPTZ,               -- NULL = active; NOT NULL = soft deleted
```

> **Exception:** `order_line_items`, `variants`, `inventory_levels` KHÔNG có `_deleted_at` (child tables, không soft-delete).

### §6.2 Bảng `orders`

| Column | Type | Ghi chú |
|---|---|---|
| `id` | BIGINT PK | Haravan order ID |
| `order_number` | INTEGER | |
| `name` | VARCHAR(64) | Order name (#1001) |
| `email` | VARCHAR(256) | |
| `financial_status` | VARCHAR(32) | paid / pending / ... |
| `fulfillment_status` | VARCHAR(32) | |
| `total_price` | NUMERIC(12,2) | |
| `subtotal_price` | NUMERIC(12,2) | |
| `total_tax` | NUMERIC(12,2) | |
| `currency` | VARCHAR(8) | |
| `customer_id` | BIGINT | FK to customers |
| `shipping_city` | VARCHAR(128) | Flatten từ `shipping_address.city` |
| `shipping_province` | VARCHAR(128) | |
| `shipping_country_code` | VARCHAR(8) | |
| `shipping_phone` | VARCHAR(32) | |
| `customer_email` | VARCHAR(256) | Flatten từ `customer.email` |
| `customer_phone` | VARCHAR(32) | |
| `transactions` | JSONB | |
| `tax_lines` | JSONB | |
| `discount_codes` | JSONB | |
| `fulfillments` | JSONB | |
| `note_attributes` | JSONB | |
| `created_at` | TIMESTAMPTZ | |
| `updated_at` | TIMESTAMPTZ | |
| `cancelled_at` | TIMESTAMPTZ | |
| `closed_at` | TIMESTAMPTZ | |

**Indexes:** `idx_orders_customer` (customer_id), `idx_orders_updated` (updated_at), `idx_orders_active` (id WHERE _deleted_at IS NULL), `idx_orders_sync_id_active` (_sync_id WHERE _deleted_at IS NULL)

### §6.3 Bảng `order_line_items`

| Column | Type |
|---|---|
| `id` | BIGINT PK |
| `order_id` | BIGINT NOT NULL |
| `product_id` | BIGINT |
| `variant_id` | BIGINT |
| `sku` | VARCHAR(128) |
| `title` | TEXT |
| `quantity` | INTEGER |
| `price` | NUMERIC(12,2) |
| `total_discount` | NUMERIC(12,2) |

### §6.4 Bảng `customers`

| Column | Type |
|---|---|
| `id` | BIGINT PK |
| `email` | VARCHAR(256) |
| `phone` | VARCHAR(32) |
| `first_name` | VARCHAR(128) |
| `last_name` | VARCHAR(128) |
| `total_spent` | NUMERIC(12,2) |
| `orders_count` | INTEGER |
| `accepts_marketing` | BOOLEAN |
| `tags` | TEXT |
| `created_at` | TIMESTAMPTZ |
| `updated_at` | TIMESTAMPTZ |

### §6.5 Bảng `products`

| Column | Type |
|---|---|
| `id` | BIGINT PK |
| `title` | TEXT |
| `vendor` | VARCHAR(128) |
| `product_type` | VARCHAR(128) |
| `handle` | VARCHAR(256) |
| `tags` | TEXT |
| `status` | VARCHAR(32) |
| `created_at` | TIMESTAMPTZ |
| `updated_at` | TIMESTAMPTZ |
| `published_at` | TIMESTAMPTZ |

### §6.6 Bảng `variants`

| Column | Type |
|---|---|
| `id` | BIGINT PK |
| `product_id` | BIGINT NOT NULL |
| `sku` | VARCHAR(128) |
| `title` | TEXT |
| `price` | NUMERIC(12,2) |
| `compare_at_price` | NUMERIC(12,2) |
| `inventory_item_id` | BIGINT |
| `inventory_quantity` | INTEGER |
| `created_at` | TIMESTAMPTZ |
| `updated_at` | TIMESTAMPTZ |

### §6.7 Bảng `inventory_levels`

| Column | Type | Ghi chú |
|---|---|---|
| `inventory_item_id` | BIGINT | PK (composite) |
| `location_id` | BIGINT | PK (composite) |
| `available` | INTEGER | |
| `updated_at` | TIMESTAMPTZ | |

### §6.8 Bảng `locations`

| Column | Type |
|---|---|
| `id` | BIGINT PK |
| `name` | VARCHAR(256) |
| `address1` | TEXT |
| `address2` | TEXT |
| `city` | VARCHAR(128) |
| `province` | VARCHAR(128) |
| `country_code` | VARCHAR(8) |
| `phone` | VARCHAR(32) |
| `active` | BOOLEAN |
| `created_at` | TIMESTAMPTZ |
| `updated_at` | TIMESTAMPTZ |

---

## §7. Haravan API Client

### §7.1 HTTP Client

```python
class HaravanClient:
    base_url = f"https://{shop_domain}/admin"
    headers = {
        "X-Haravan-Access-Token": access_token,
        "Content-Type": "application/json",
    }
    timeout = 30.0  # seconds
```

### §7.2 Token Bucket (Leaky Bucket)

```
CAPACITY     = 80     # Max tokens Haravan allows
LEAK_RATE    = 4.0    # tokens/second được restore
SAFETY_MARGIN = 70    # Block acquire() nếu _tokens_used >= 70
```

**Algorithm:**
```
acquire():
    while True:
        with lock:
            _leak()           # Reduce tokens by elapsed_time * LEAK_RATE
            if _tokens_used < SAFETY_MARGIN:
                _tokens_used += 1
                return
        sleep(0.1)            # Chờ và retry

_leak():
    elapsed = now - _last_leak
    _tokens_used = max(0.0, _tokens_used - elapsed * LEAK_RATE)
    _last_leak = now

update_from_header("45/80"):
    _tokens_used = 45.0       # Override từ server response
```

**Cold-start prime:** Gọi `GET /shop.json` khi init, đọc `X-Haravan-Api-Call-Limit` header. Nếu fail → set `_tokens_used = 50.0` (conservative fallback).

### §7.3 Retry Strategy

```
max_attempts = 5
wait = exponential(multiplier=1, min=2s, max=60s)
retry_if = HaravanRateLimited | HaravanServerError | HaravanNetworkError
```

**Status code mapping:**
- `429` → `HaravanRateLimited(retry_after=float(Retry-After header, default=5))`
- `401`, `403` → `HaravanAuthError` (không retry)
- `5xx` → `HaravanServerError` (retry)
- `4xx` khác → `HaravanClientError` (không retry)
- Network/Timeout → `HaravanNetworkError` (retry)

### §7.4 Keyset Pagination

```
paginate(resource, params, page_size=250, checkpoint=(last_ts, last_id)):
    params = {**params, "limit": 250, "updated_at_min": last_ts - 5min}
    
    loop:
        records = GET /{resource}.json?{params}
        if empty → stop
        
        for r in records:
            if (r.updated_at, r.id) <= (last_ts, last_id):
                skip  # 5-min overlap dedup
            yield r
            update (last_ts, last_id) = (r.updated_at, r.id)
        
        if len(records) < page_size → stop (last page)
        if no_new_any → warning "pagination_stuck" → stop
        
        params["updated_at_min"] = last_ts  # advance cursor
```

---

## §8. Transform Engine

### §8.1 Flattening Rules per Resource

**`orders`:**
- Flat fields: `id`, `order_number`, `name`, `email`, `financial_status`, `fulfillment_status`, `total_price`, `subtotal_price`, `total_tax`, `currency`, `customer_id`, `created_at`, `updated_at`, `cancelled_at`, `closed_at`
- Nested flatten `shipping_address` → prefix `shipping_`: `city`, `province`, `country_code`, `phone`
- Nested flatten `customer` → prefix `customer_`: `email`, `phone`
- Nested JSONB: `transactions`, `tax_lines`, `discount_codes`, `fulfillments`, `note_attributes`
- Child extract: `line_items` → `order_line_items` table (separate upsert)

**`customers`:** flat fields + meta columns

**`products`:** flat fields + meta columns

**`variants`:** flat fields (không có `_deleted_at`)

**`inventory_levels`:** `(inventory_item_id, location_id)` composite PK

### §8.2 apply_transform()

```python
def apply_transform(record, transform_config, resource) -> dict:
    # 1. Validate id
    if not record.get("id") and resource not in ("inventory_levels",):
        raise InvalidRecordError(f"Record missing id in resource {resource!r}")
    
    # 2. Flat fields với rename/skip/type_override
    # 3. Nested flatten
    # 4. Nested JSONB
    # 5. Meta prefix flatten (nested_prefix)
    
    return out  # dict với _raw = original record
```

### §8.3 Type Override / `_cast()`

| `pg_type` pattern | Python conversion |
|---|---|
| `numeric`, `decimal`, `float` | `Decimal(str(value))` |
| `integer`, `int`, `bigint`, `smallint` | `int(value)` |
| `boolean` | `bool` hoặc `str.lower() in ("true","1","yes")` |
| `timestamp...` | `datetime.fromisoformat()` → UTC |
| Fail | Log warning, **return `None`** |

---

## §9. Load Engine

### §9.1 Upsert Strategy

```sql
INSERT INTO {table} (col1, col2, ..., _raw, _synced_at, _sync_id)
VALUES (...)
ON CONFLICT (id) DO UPDATE SET
    col1 = EXCLUDED.col1,
    ...
    _raw = EXCLUDED._raw,
    _synced_at = EXCLUDED._synced_at,
    _sync_id = EXCLUDED._sync_id
    -- _deleted_at KHÔNG cập nhật để preserve soft-delete state
```

### §9.2 Soft Delete (Full Refresh only)

```
Điều kiện thực hiện: mode == "full_refresh" AND resource IN soft_delete_resources

1. rows_before = COUNT(*) WHERE _deleted_at IS NULL
2. Run sync, collect set of fetched IDs
3. would_delete = rows_before - len(fetched_ids)
4. delete_pct = would_delete / rows_before * 100
5. if delete_pct > 5%:
       → ValidationResult "soft_delete_sanity" FAILED
       → ABORT sweep (không set _deleted_at)
   else:
       → UPDATE SET _deleted_at = NOW() WHERE id NOT IN (fetched_ids)
```

**Soft-delete resources:** `orders`, `customers`, `products`, `locations`  
**Non-soft-delete:** `order_line_items`, `variants`, `inventory_levels` (child tables)

---

## §10. Sync Runner

### §10.1 SyncResult

```python
@dataclass
class SyncResult:
    sync_id: UUID
    pipeline: str
    resource: str
    mode: str
    trigger: str           # "schedule" | "manual"
    status: str            # "success" | "failed"
    fetched_count: int
    upserted_count: int
    skipped_invalid: int
    soft_deleted_count: int
    rows_before: int
    rows_after: int
    duration_seconds: float
    error: str | None
```

### §10.2 Run Flow

```
run_sync(pipeline, resource, mode, trigger):
    1. sync_id = uuid4()
    2. rows_before = COUNT destination rows
    3. checkpoint = load checkpoint (nếu incremental)
    4. Extract: yield records từ Haravan
    5. Transform: apply_transform() per record
       → InvalidRecordError → skip + skipped_invalid++
    6. Load: upsert vào destination
    7. Soft-delete sweep (nếu full_refresh)
    8. rows_after = COUNT destination rows
    9. Validate: run_all_validations(ctx)
    10. Save checkpoint
    11. Record sync_run
    12. Alert nếu validation failed
```

---

## §11. Scheduler

### §11.1 APScheduler Config

```python
scheduler = BackgroundScheduler(
    executors={"default": ThreadPoolExecutor(max_workers=1)},
    job_defaults={"coalesce": True, "max_instances": 1},
    timezone="Asia/Ho_Chi_Minh",
)
```

> `max_workers=1` đảm bảo không có 2 sync jobs chạy song song (tránh upsert conflict).

### §11.2 Default Schedules

| Resource | Incremental | Weekly Full Refresh |
|---|---|---|
| `orders` | `0 */2 * * *` (mỗi 2h, phút 0) | `0 3 * * 0` |
| `customers` | `5 */2 * * *` (mỗi 2h, phút 5) | `30 3 * * 0` |
| `products` | `10 */12 * * *` (mỗi 12h, phút 10) | `0 4 * * 0` |
| `variants` | `10 */12 * * *` (paired với products) | `0 4 * * 0` |
| `inventory_levels` | `15 */2 * * *` (mỗi 2h, phút 15) | `30 4 * * 0` |
| `locations` | `20 0 * * *` (midnight, phút 20) | `0 5 * * 0` |

> **Staggered schedules** để tránh 2 jobs fire đồng thời (max_workers=1). `variants` paired với `products` — không flag schedule collision.

### §11.3 Reconciler

```
every 10 seconds:
    1. Load enabled pipelines từ DB
    2. So sánh với scheduled jobs hiện tại
    3. Add/remove jobs cho pipelines mới/bị xóa
    4. Update heartbeat
```

---

## §12. Validation Rules

| Rule | Trigger | Thresholds | Hành động khi fail |
|---|---|---|---|
| **R1 — fetch_upsert_parity** | Mọi sync | skip_pct > 1% → failed | Alert |
| **R2 — volume_sanity** | Incremental only | 3 lần liên tiếp fetch = 0 → warning | Alert |
| **R3 — weekly_full_drift** | Full refresh | drift > 5% → warning; > 20% → failed | Alert |
| **R4 — soft_delete_sanity** | Full refresh | delete_pct > 5% → failed | Block sweep + Alert |

---

## §13. Alerting

### §13.1 Telegram Integration

```python
class TelegramSender:
    _url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    def send(message):
        _send_with_retry(message)  # 3 attempts
```

### §13.2 Deduplication

```python
class AlertDeduper:
    ttl = 1800  # 30 minutes
    
    def should_send(alert_key) -> bool:
        if alert_key in _sent_within_ttl:
            return False
        record(alert_key, now)
        return True
```

> Alert key = `(pipeline, resource, rule)` — cùng loại alert, cùng resource không gửi lại trong 30 phút.

---

## §14. Logging

### §14.1 Structlog Pipeline

```
event_dict → add_log_level → add_timestamp → add_pipeline_context
          → deep_redact (mask sensitive keys)
          → JSONRenderer → stdout
          → DBSink (async batch insert vào sync_logs)
```

### §14.2 Sensitive Key Redaction

Keys bị mask: `token`, `password`, `secret`, `api_key`, `access_token`, `authorization`, `ciphertext`

Pattern trong value strings: `(token|password|secret|api_key|bearer)\s*[:=]\s*<value>`

### §14.3 Event Names (EventName enum)

`SYNC_STARTED`, `SYNC_COMPLETED`, `SYNC_FAILED`, `RECORD_SKIPPED`, `CHECKPOINT_LOADED`, `CHECKPOINT_SAVED`, `UPSERT_BATCH_DONE`, `UPSERT_CONFLICT`, `SOFT_DELETE_SWEEP_DONE`, `SOFT_DELETE_SWEEP_ABORTED`, `VALIDATION_FAILED`, `ALERT_SENT`, `ALERT_FAILED`, `BUCKET_ACQUIRE_BLOCKED`, `TYPE_OVERRIDE_FAILED`, `PAGINATION_STUCK`

---

## §15. Retention & Cleanup

```python
cleanup_old_logs(engine, retention_days=14):
    DELETE FROM sync_logs WHERE timestamp < NOW() - INTERVAL '{days} days'
    
cleanup_old_runs(engine, retention_days=90):
    DELETE FROM sync_runs WHERE finished_at < NOW() - INTERVAL '{days} days'
```

Chạy tự động mỗi ngày lúc 03:00 (Asia/Ho_Chi_Minh).

---

## §16. CLI Commands

```bash
# Bootstrap
flowbyte bootstrap                        # First-time setup

# Pipeline management
flowbyte init <name>                      # Tạo YAML template
flowbyte validate <name>                  # Test connections
flowbyte enable <name>                    # Enable + upsert vào DB
flowbyte disable <name>                   # Disable scheduler
flowbyte delete <name> [--force]          # Xóa pipeline
flowbyte list                             # Liệt kê tất cả pipelines

# Credentials
flowbyte creds set <ref> --kind haravan   # Lưu Haravan credentials
flowbyte creds set <ref> --kind postgres  # Lưu Postgres password
flowbyte creds list                       # Xem credentials (không hiện secret)
flowbyte creds delete <ref>               # Xóa credentials

# Manual sync
flowbyte sync run <pipeline> <resource>   # Trigger manual sync
flowbyte sync run <pipeline> <resource> --mode full_refresh

# Observability
flowbyte obs summary [--pipeline <name>]  # 24h summary
flowbyte obs logs <pipeline>              # Recent logs
flowbyte obs runs <pipeline>              # Sync run history

# Alerting
flowbyte alerting test                    # Send test message
flowbyte alerting enable
flowbyte alerting disable

# System
flowbyte system status                    # Daemon health check
flowbyte system daemon                    # Start daemon (foreground)
```

---

## §17. Test Strategy

### §17.1 Unit Tests

| File | Scope |
|---|---|
| `test_token_bucket.py` | Leak algorithm, thread safety, SAFETY_MARGIN blocking, header parsing |
| `test_encryption.py` | AES-256-GCM encrypt/decrypt, AAD binding, tamper detection |
| `test_config_models.py` | Pydantic validation, schedule collision detection, transform config |
| `test_transform.py` | apply_transform flattening, type casting, rename/skip, InvalidRecordError |
| `test_validation_rules.py` | 4 validation rules: R1-R4 với các threshold |
| `test_internal_schema.py` | SQLAlchemy table definitions, column types, view SQL |
| `test_retry_predicates.py` | should_retry() cho từng exception type |
| `test_alert_deduper.py` | Deduplication TTL logic |

### §17.2 Integration Tests

| File | Scope |
|---|---|
| `test_schema.py` | Migration 001 tạo đủ 11 tables + materialized view; roundtrip upgrade/downgrade/upgrade |

### §17.3 Test Fixtures

- `pg_container` (session-scoped): PostgreSQL container via testcontainers
- `pg_conn` / `dest_conn`: psycopg connection tới internal / destination DB

### §17.4 Float Equality trong Tests

Token bucket tests dùng tolerance thay vì exact equality:
```python
assert abs(bucket._tokens_used - expected) < 0.01  # single acquire
assert abs(bucket._tokens_used - 10.0) < 0.1       # concurrent 10 threads
```

Lý do: sub-millisecond time leakage giữa các `acquire()` calls gây floating-point drift.

### §17.5 Thread Safety

`test_acquire_blocks_at_safety_margin` set `_tokens_used = SAFETY_MARGIN + 1.0` (không phải đúng SAFETY_MARGIN) để tránh race condition với leakage trước khi thread chạy.

---

## §18. Dependencies

```toml
[dependencies]
httpx = ">=0.28"
tenacity = ">=9.0"
sqlalchemy = ">=2.0"
psycopg = {extras = ["binary"], version = ">=3.1"}
alembic = ">=1.13"
pydantic = ">=2.0"
pydantic-settings = ">=2.0"
structlog = ">=24.0"
apscheduler = ">=3.10"
croniter = ">=3.0"
ruamel.yaml = ">=0.19"
typer = ">=0.12"
rich = ">=13.0"
cryptography = ">=42.0"

[dev-dependencies]
pytest = ">=8.0"
pytest-timeout = "*"
freezegun = "*"
testcontainers = {extras = ["postgres"], version = ">=4.0"}
```

---

## §19. Lịch sử thay đổi

| Version | Ngày | Thay đổi |
|---|---|---|
| 1.0 | 2026-04-24 | Phiên bản đầu, Sprint 1 |
