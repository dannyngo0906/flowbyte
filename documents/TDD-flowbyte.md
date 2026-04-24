# TDD — Flowbyte

> **Version:** 1.1 · **Ngày:** 2026-04-24 · **Author:** Duy
> **Status:** Ready for implementation (with v1.1 amendments)
> Technical Design Document cho Flowbyte (MVP CLI-only).
> Nguồn gốc: `PRD-flowbyte.md` v2 + 8 quyết định kiến trúc đã chốt trong phiên phỏng vấn tech 2026-04-24.
> Mỗi section có dòng **💡 Tại sao** để dễ nhớ về sau.
>
> ⚠️ **v1.1 UPDATE:** Section §17 chứa 11 amendments từ senior review. Đọc §17 **TRƯỚC** khi implement — nó override các code example ở §6.2, §7.3, §7.4, §8.3, §8.4, §8.5, §10.5. Callout `⚠️ AMENDED v1.1 → §17.X` đánh dấu từng chỗ.

---

## ① Overview

### 1.1 Mục tiêu kỹ thuật
Xây một ETL tool CLI-first, self-hosted, sync **Haravan Omnichannel API → PostgreSQL** trên VPS dùng chung với n8n, memory ≤ 1.5 GB, chi phí ≤ $9/tháng.

### 1.2 Phạm vi MVP
6 resources Phase 1: `orders`, `customers`, `products`, `variants`, `inventory_levels`, `locations`.
8 MUST features (F1–F8 trong PRD). Không bao gồm Web UI, multi-shop, transformation phức tạp.

### 1.3 Tám quyết định kiến trúc đã chốt

| # | Quyết định | 💡 Tại sao |
|---|-----------|-----------|
| **A1** | **APScheduler in-process** (không cron hệ thống, không Celery) | 1 process duy nhất → debug dễ, recovery state qua jobstore Postgres, không phụ thuộc broker. Solo dev + AI tools không cần scale-out. |
| **A2** | **DB polling qua control plane tables** (CLI↔Daemon qua Postgres, không HTTP/Unix socket) | Không cần mở thêm port/socket, audit trail miễn phí, CLI chạy bất cứ đâu đọc được DB là đủ. Trade-off latency ~1-2s chấp nhận được cho CLI. |
| **A3** | **SQLAlchemy Core 2.0 + psycopg3 + Alembic** (không ORM) | Core Table/Query đủ rõ, không magic lazy-load; psycopg3 async-ready; Alembic migration versioned. Khớp với persona Data Analyst quen SQL. |
| **A4** | **httpx + tenacity + custom HaravanTokenBucket** | httpx HTTP/2 + timeout mịn; tenacity khai báo retry decorator; token bucket tự code để đồng bộ chính xác với header `X-Haravan-Api-Call-Limit`. |
| **A5** | **Pydantic v2 + pydantic-settings + ruamel.yaml + croniter** | Pydantic v2 validate + type cast 1 lần đầu; ruamel.yaml preserve comment (quan trọng cho Git review); croniter parse cron hợp chuẩn. |
| **A6** | **structlog + stdlib bridge + 3 sinks** (console + rotating file + `sync_logs` DB processor) | structlog context binding giữ pipeline/resource/sync_id xuyên suốt; stdlib bridge để SQLAlchemy/httpx log cùng format; DB processor cho `flowbyte logs`. |
| **A7** | **Hybrid 2-tier testing: unit (pure) + integration (testcontainers + respx + freezegun)** | Unit nhanh để TDD vòng ngắn; integration dựng Postgres thật + mock Haravan HTTP để bắt bug transaction/encoding không thấy ở unit. |
| **A8** | **Single container + `docker exec` CLI + auto-migrate on boot + master key mount host** | Một container `flowbyte` chạy daemon; CLI vào bằng `docker exec flowbyte flowbyte <cmd>`; Alembic auto-upgrade khi container start; master key host-mount `ro`. |

### 1.4 Architectural constraints & defaults

| Default | Giá trị | Lý do |
|---|---|---|
| Python | 3.11+ | Match CPython wheels psycopg3, structlog, pydantic v2 |
| Postgres | ≥ 14 | JSONB GIN index, generated columns |
| Timezone internal | UTC | Postgres `timestamptz`; convert khi display |
| Line endings | LF | Git portable |
| Encoding | UTF-8 everywhere | Haravan trả UTF-8 |
| Container memory limit | 1.5 GB | Bảo vệ n8n cùng VPS |

**💡 Tại sao một section defaults:** Giữ mọi "hằng số môi trường" tại 1 chỗ → cần đổi (vd nâng Python 3.12) chỉ đọc 1 bảng.

---

## ② System Architecture

### 2.1 High-level diagram

```
┌────────────────────────────────────────────────────────────────────┐
│                     Flowbyte Container (1.5GB)                      │
│                                                                     │
│  ┌─────────────┐        ┌──────────────────────────────────────┐   │
│  │  CLI        │        │  Daemon (long-running)               │   │
│  │  (Typer)    │        │                                      │   │
│  │             │◄──poll─┤  ┌──────────────────────────────┐    │   │
│  │  docker exec│        │  │ APScheduler                  │    │   │
│  └──────┬──────┘        │  │  - jobstore: Postgres        │    │   │
│         │               │  │  - trigger per resource      │    │   │
│         │               │  │  - weekly full Sun 02:00     │    │   │
│         │               │  └─────┬────────────────────────┘    │   │
│         │               │        │                              │   │
│         │               │  ┌─────▼────────────────────────┐    │   │
│         │               │  │ SyncRunner                   │    │   │
│         │               │  │  HaravanClient ─► Transform  │    │   │
│         │               │  │                    ─► Load   │    │   │
│         │               │  └─────┬────────────────────────┘    │   │
│         │               │        │                              │   │
│         │               │  ┌─────▼────────────────────────┐    │   │
│         │               │  │ Reconciler (control plane)    │    │   │
│         │               │  │  - watches trigger_commands   │    │   │
│         │               │  │  - heartbeat every 30s        │    │   │
│         │               │  └──────────────────────────────┘    │   │
│         │               └─────────────┬────────────────────────┘   │
│         │                             │                             │
│         │  ┌──────────────────────────▼─────────────────────────┐  │
│         └─►│         PostgreSQL (2 logical databases)            │  │
│            │                                                     │  │
│            │  flowbyte_internal                                  │  │
│            │  ├ credentials (encrypted)                          │  │
│            │  ├ pipelines                                        │  │
│            │  ├ sync_checkpoints                                 │  │
│            │  ├ sync_logs                                        │  │
│            │  ├ validation_results                               │  │
│            │  ├ trigger_commands   ◄── CLI writes               │  │
│            │  ├ daemon_heartbeat   ◄── Daemon writes            │  │
│            │  └ apscheduler_jobs   ◄── APScheduler jobstore     │  │
│            │                                                     │  │
│            │  flowbyte_destination                               │  │
│            │  ├ orders, order_line_items                         │  │
│            │  ├ customers, products, variants                    │  │
│            │  └ inventory_levels, locations                      │  │
│            └─────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
                               ▲                    │
                               │                    │
                          ┌────┴───────┐       ┌───▼──────────┐
                          │  Haravan   │       │  Telegram    │
                          │  API       │       │  Bot         │
                          └────────────┘       └──────────────┘
```

### 2.2 Components

| Component | Trách nhiệm | Code location |
|---|---|---|
| **CLI** | Parse command, ghi lệnh vào `trigger_commands`, đọc kết quả | `flowbyte/cli/` |
| **Daemon** | Long-running process: scheduler + reconciler + heartbeat | `flowbyte/daemon.py` |
| **Scheduler** | APScheduler với Postgres jobstore, trigger sync theo cron | `flowbyte/scheduling/` |
| **SyncRunner** | Pipeline Extract→Transform→Load cho 1 resource | `flowbyte/sync/runner.py` |
| **HaravanClient** | HTTP + rate limit + retry + pagination | `flowbyte/haravan/` |
| **Transformer** | YAML field mapping, flatten nested, JSONB arrays | `flowbyte/sync/transform.py` |
| **Loader** | Upsert `ON CONFLICT`, atomic checkpoint | `flowbyte/sync/loader.py` |
| **Reconciler** | Poll `trigger_commands` mỗi 2s, dispatch action | `flowbyte/daemon.py::Reconciler` |
| **Alerter** | Telegram send với dedup + rate limit | `flowbyte/alerting/` |
| **Config** | Parse YAML + pydantic validate + env overlay | `flowbyte/config/` |

### 2.3 Tech stack

| Layer | Library | Version | Lý do chọn |
|---|---|---|---|
| CLI framework | **Typer** + Rich | 0.12+ | Auto help, type hints → args; Rich pretty table cho `status` |
| HTTP client | **httpx** | 0.27+ | HTTP/2, timeout fine-grained, sync/async cùng API |
| Retry | **tenacity** | 8.2+ | Decorator declarative, stop/wait composable |
| DB driver | **psycopg** (v3) | 3.1+ | Async native, server-side cursor |
| SQL toolkit | **SQLAlchemy Core** | 2.0+ | Table + select() rõ ràng, không ORM magic |
| Migration | **Alembic** | 1.13+ | Version SQL, auto-upgrade on boot |
| Scheduler | **APScheduler** | 3.10+ | Cron trigger, Postgres jobstore recovery |
| Config model | **Pydantic v2** + pydantic-settings | 2.6+ | Validate YAML, env var overlay |
| YAML | **ruamel.yaml** | 0.18+ | Preserve comments (Git review dễ) |
| Cron parse | **croniter** | 2.0+ | Compute next_fire_time cho `status` |
| Logging | **structlog** + stdlib bridge | 24.0+ | Context vars, JSON/console renderer |
| Encryption | **cryptography** | 42+ | AES-256-GCM audited |
| HTTP mock (test) | **respx** | 0.21+ | Match httpx transport layer |
| DB test | **testcontainers[postgres]** | 4.0+ | Postgres 14 thật trong Docker |
| Time mock | **freezegun** | 1.5+ | Lock time cho test incremental sync |
| Test runner | **pytest** + pytest-xdist + pytest-cov | 8.0+ | Parallel, coverage |
| Linter | **ruff** | 0.4+ | All-in-one lint + format |
| Type check | **mypy** | 1.9+ | Strict mode |

### 2.4 Deployment topology

```
VPS Ubuntu 22.04 (4GB RAM, 50GB disk)
├── n8n container (~1.5GB)
├── postgres-shared container (or host postgres)
└── flowbyte container (1.5GB)
     ├── mounts:
     │   ├── /etc/flowbyte/master.key  (host, chmod 600, ro)
     │   ├── /etc/flowbyte/pipelines/  (host, YAML configs, rw)
     │   └── /var/log/flowbyte/        (host, rotating logs, rw)
     ├── env:
     │   ├── FLOWBYTE_DB_URL=postgres://...
     │   └── FLOWBYTE_MASTER_KEY_PATH=/etc/flowbyte/master.key
     └── entrypoint: flowbyte daemon-start (auto migrate + APScheduler)
```

**💡 Tại sao single container + mount master key ngoài image:** Image không bao giờ chứa secret → `docker save` / push registry an toàn. Key rotation = đổi file host + restart container.

---

## ③ Data Model

### 3.1 ERD tổng quan

```
Internal DB (flowbyte_internal)
────────────────────────────────
pipelines ─1──N─ sync_checkpoints (PK: pipeline, resource)
    │
    └1──N─ sync_logs (PK: id BIGSERIAL)
    └1──N─ validation_results
    └1──N─ trigger_commands          ◄── CLI writes
    └1──N─ daemon_heartbeat (1 row)  ◄── Daemon writes

credentials (standalone, encrypted BYTEA)
apscheduler_jobs (managed by APScheduler)

Destination DB (flowbyte_destination)
────────────────────────────────────
orders ─1──N─ order_line_items
products ─1──N─ variants
customers
inventory_levels (PK: inventory_item_id + location_id)
locations
```

### 3.2 SQL schema — Internal DB

```sql
-- credentials (AES-256-GCM ciphertext)
CREATE TABLE credentials (
  ref             TEXT PRIMARY KEY,
  encrypted_blob  BYTEA NOT NULL,
  nonce           BYTEA NOT NULL,           -- 12 bytes per GCM spec
  created_at      TIMESTAMPTZ DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT now()
);

-- pipelines (1 row per YAML config)
CREATE TABLE pipelines (
  name            TEXT PRIMARY KEY,
  enabled         BOOLEAN DEFAULT true,
  yaml_path       TEXT NOT NULL,
  yaml_hash       TEXT NOT NULL,            -- sha256 để detect edit ngoài CLI
  created_at      TIMESTAMPTZ DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT now()
);

-- sync_checkpoints (incremental watermark)
CREATE TABLE sync_checkpoints (
  pipeline_name   TEXT NOT NULL,
  resource        TEXT NOT NULL,
  checkpoint_ts   TIMESTAMPTZ NOT NULL,
  last_sync_id    BIGINT,                   -- link to sync_logs.id
  PRIMARY KEY (pipeline_name, resource)
);

-- sync_logs (history + structured logging sink)
CREATE TABLE sync_logs (
  id              BIGSERIAL PRIMARY KEY,
  pipeline_name   TEXT NOT NULL,
  resource        TEXT NOT NULL,
  sync_mode       TEXT NOT NULL,            -- 'incremental' | 'full_refresh'
  status          TEXT NOT NULL,            -- 'running' | 'success' | 'failed' | 'skipped'
  records_synced  INTEGER,
  duration_ms     INTEGER,
  error_message   TEXT,
  error_stack     TEXT,
  started_at      TIMESTAMPTZ NOT NULL,
  finished_at     TIMESTAMPTZ,
  log_level       TEXT,                     -- for structlog DB sink rows: 'INFO'|'WARN'|'ERROR'
  log_event       JSONB,                    -- event + context vars
  created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_sync_logs_pipeline ON sync_logs (pipeline_name, created_at DESC);
CREATE INDEX idx_sync_logs_status_time ON sync_logs (status, created_at DESC)
  WHERE status IN ('failed', 'running');

-- validation_results
CREATE TABLE validation_results (
  id              BIGSERIAL PRIMARY KEY,
  pipeline_name   TEXT NOT NULL,
  resource        TEXT NOT NULL,
  sync_id         BIGINT REFERENCES sync_logs(id),
  rule            TEXT NOT NULL,            -- 'row_count_delta' | 'required_fields_not_null'
  status          TEXT NOT NULL,            -- 'passed' | 'warning' | 'failed'
  details         JSONB,
  created_at      TIMESTAMPTZ DEFAULT now()
);

-- trigger_commands (control plane — CLI writes, daemon reads)
CREATE TABLE trigger_commands (
  id              BIGSERIAL PRIMARY KEY,
  command         TEXT NOT NULL,            -- 'run' | 'enable' | 'disable' | 'reload_config'
  pipeline_name   TEXT,
  resource        TEXT,
  args            JSONB,
  status          TEXT NOT NULL DEFAULT 'pending',  -- pending | picked | done | failed
  result          JSONB,
  created_at      TIMESTAMPTZ DEFAULT now(),
  picked_at       TIMESTAMPTZ,
  finished_at     TIMESTAMPTZ
);
CREATE INDEX idx_trigger_pending ON trigger_commands (status, created_at)
  WHERE status = 'pending';

-- daemon_heartbeat (1 row, upserted mỗi 30s)
CREATE TABLE daemon_heartbeat (
  id              INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  pid             INTEGER NOT NULL,
  version         TEXT NOT NULL,
  started_at      TIMESTAMPTZ NOT NULL,
  last_beat_at    TIMESTAMPTZ NOT NULL
);

-- apscheduler_jobs: managed by APScheduler SQLAlchemyJobStore; not edited by us
```

**💡 Tại sao `trigger_commands` thay vì HTTP/Unix socket:** Row trong DB là audit log miễn phí; CLI có thể chạy trên host khác container (ssh + docker exec hay không, đều OK); không cần mở port.

### 3.3 SQL schema — Destination DB (xem PRD ⑦ phần `orders`, `customers`, `products`, `variants`, `inventory_levels`, `locations`)

Tuân thủ đúng PRD ⑦. Bổ sung:
- Mọi bảng data có `_raw JSONB NOT NULL`, `_deleted_at TIMESTAMPTZ NULL`, `_synced_at TIMESTAMPTZ DEFAULT now()`.
- Index chiến lược:
  - `CREATE INDEX idx_<table>_updated_at` trên `updated_at` (dùng bởi incremental filter khi query history).
  - `CREATE INDEX idx_<table>_active` trên `(id) WHERE _deleted_at IS NULL` (query live).
  - Không tạo GIN cho `_raw` mặc định → nhiều storage; user bật thủ công khi cần.

### 3.4 Encryption strategy

```
Plaintext JSON credentials
         │
         ▼
AES-256-GCM(key=master_key, nonce=random 12 bytes, aad=ref)
         │
         ▼
credentials.encrypted_blob + credentials.nonce
```

- **Master key:** 32 bytes random, sinh 1 lần bởi `flowbyte init-master-key`.
- **AAD** (Additional Authenticated Data): `ref` string → chống attacker đổi `ref` rồi dán blob cũ.
- **Key rotation:** Phase 2 feature — `flowbyte rotate-master-key --old PATH --new PATH` re-encrypt tất cả rows.

**💡 Tại sao AES-256-GCM (không CBC, không Fernet chỉ 128-bit):** GCM auth tag chống tamper; 256-bit future-proof quantum gần. `cryptography.hazmat.primitives.ciphers.aead.AESGCM` API đơn giản, không pitfall IV reuse.

---

## ④ CLI Commands

### 4.1 Command catalogue

| Command | Mô tả | Flow |
|---|---|---|
| `flowbyte init-master-key` | Sinh master key 32 bytes, ghi ra path, chmod 600 | Generate random → write → chmod |
| `flowbyte init <name>` | Tạo YAML template `/etc/flowbyte/pipelines/<name>.yml` | Render Jinja → write |
| `flowbyte creds set <ref>` | Nhập access_token interactive (prompt ẩn) | Read → encrypt → upsert |
| `flowbyte creds list` | Liệt kê refs (không in plaintext) | SELECT ref, updated_at |
| `flowbyte validate <pipeline>` | Test Haravan + Postgres connection (< 5s) | YAML parse → ping DB → GET /shop.json |
| `flowbyte list` | Bảng pipelines | SELECT * + JOIN sync_logs last |
| `flowbyte enable <pipeline>` | INSERT `trigger_commands` enable | Daemon reconciler add jobs |
| `flowbyte disable <pipeline>` | INSERT `trigger_commands` disable | Daemon remove jobs |
| `flowbyte run <pipeline> [--resource R]` | Trigger sync thủ công | INSERT trigger `run`, poll result |
| `flowbyte status` | Overview pipelines + resources | SELECT + Rich table |
| `flowbyte history <pipeline> [--last 10]` | Lịch sử sync | SELECT sync_logs ORDER BY id DESC |
| `flowbyte logs <pipeline> [--tail\|--errors\|--since 1h]` | Log stream | SELECT sync_logs WHERE log_event IS NOT NULL |
| `flowbyte delete <pipeline>` | Xóa pipeline (confirm) | DELETE + archive YAML |
| `flowbyte alert test` | Gửi Telegram test message | POST sendMessage |
| `flowbyte cleanup [--dry-run]` | Retention cleanup thủ công | DELETE + VACUUM ANALYZE |
| `flowbyte migrate` | Alembic upgrade head | `alembic upgrade head` |
| `flowbyte daemon-start` | Entry point container | Start scheduler + reconciler loop |

### 4.2 Command lifecycle cho `flowbyte run shop_main`

```
1. CLI
   └─ INSERT INTO trigger_commands (command='run', pipeline='shop_main', status='pending')
      RETURNING id AS cmd_id;
2. CLI polls
   └─ SELECT status, result FROM trigger_commands WHERE id=cmd_id EVERY 1s UNTIL status IN ('done','failed') OR 600s timeout
3. Daemon Reconciler (poll loop 2s)
   └─ SELECT * FROM trigger_commands WHERE status='pending' ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED
   └─ UPDATE ... SET status='picked', picked_at=now()
   └─ dispatch(command) → SyncRunner.run(pipeline, resource)
   └─ UPDATE ... SET status='done'|'failed', result=jsonb, finished_at=now()
4. CLI prints result, exit(0 on done, 1 on failed)
```

**💡 Tại sao `FOR UPDATE SKIP LOCKED`:** Nếu scale ra nhiều daemon replica (phase 2), lệnh không double-processed; với 1 daemon hiện tại cũng không lỗi.

### 4.3 Exit codes

| Code | Ý nghĩa |
|---|---|
| 0 | Success |
| 1 | Generic failure (xem stderr) |
| 2 | Config/CLI argument error |
| 3 | Validation failed (creds sai, connection fail) |
| 4 | Timeout waiting for daemon |
| 5 | Daemon not running / heartbeat stale |

---

## ⑤ Credentials & Encryption

### 5.1 Lifecycle của master key

```
Install:
  flowbyte init-master-key
    ├─ generate 32 random bytes (secrets.token_bytes(32))
    ├─ write to /etc/flowbyte/master.key
    ├─ chmod 600
    └─ prompt: "Back up this file OUTSIDE the VPS now"

Runtime:
  Daemon.__init__:
    ├─ read MASTER_KEY_PATH (default /etc/flowbyte/master.key)
    ├─ validate length == 32
    └─ hold in memory (never log, never write elsewhere)

Access creds:
  decrypt(encrypted_blob, nonce, aad=ref, key=master_key)

Rotate (phase 2):
  flowbyte rotate-master-key --old PATH --new PATH
    ├─ for each row in credentials:
    │   decrypt with old → re-encrypt with new → UPDATE
    └─ atomic per-row; resume on crash
```

### 5.2 Set credentials flow

```
$ flowbyte creds set shop_main_creds
Enter Haravan access token: ****************
Confirm: ****************
Shop domain [mycompany.myharavan.com]: _

└─ build JSON: {"access_token": "...", "shop_domain": "..."}
└─ nonce = os.urandom(12)
└─ blob = AESGCM(key).encrypt(nonce, json.dumps(payload).encode(), aad=ref.encode())
└─ INSERT INTO credentials(ref, encrypted_blob, nonce) ON CONFLICT (ref) DO UPDATE
```

### 5.3 Threat model

| Threat | Mitigation |
|---|---|
| Master key bị lộ từ image Docker | Không bao giờ COPY vào image; mount host volume `:ro` |
| DB dump rò rỉ credentials | Ciphertext + GCM tag; không có key thì vô dụng |
| Attacker swap `ref` | AAD binding `ref` → GCM tag invalid khi verify |
| Master key mất | **User responsibility**: CLI nhắc backup; credentials bất khả thu hồi → user `creds set` lại |
| Log mask bị bypass | `SecretStr` Pydantic; repr() ra `**********` |

---

## ⑥ Scheduling Engine

### 6.1 APScheduler architecture

```python
BackgroundScheduler(
    jobstores={"default": SQLAlchemyJobStore(url=INTERNAL_DB_URL)},
    executors={"default": ThreadPoolExecutor(max_workers=1)},  # 1 để token bucket shared đơn giản
    job_defaults={
        "coalesce": True,          # miss 3 fires → chỉ chạy 1
        "max_instances": 1,        # pipeline-level lock: 1 resource không chạy song song chính nó
        "misfire_grace_time": 600, # 10 phút
    },
    timezone=pytz.UTC,
)
```

**💡 Tại sao `max_workers=1` + `max_instances=1`:** Đơn giản hóa concurrency; 1 token bucket Haravan shared tự nhiên không race; resource queue tuần tự (PRD §⑧ concurrency).

### 6.2 Job mapping

> ⚠️ **AMENDED v1.1 → §17.3, §17.7:** Bảng bên dưới (a) hardcode timezone UTC (weekly full `"0 2 * * 0"` = 9 AM giờ VN = peak traffic), (b) thiếu schedule stagger giữa các resource → cold start dồn tải. Phải dùng `timezone: Asia/Ho_Chi_Minh` từ config + stagger mặc định. Đọc §17.3 & §17.7 trước khi implement.

| Job ID pattern | Trigger | Job func |
|---|---|---|
| `sync:{pipeline}:{resource}` | `CronTrigger.from_crontab(resource.schedule)` | `SyncRunner.run(pipeline, resource, mode)` |
| `weekly_full:{pipeline}` | `CronTrigger.from_crontab("0 2 * * 0")` | `SyncRunner.run_all(pipeline, mode="full_refresh")` |
| `cleanup` | `CronTrigger.from_crontab("0 3 * * *")` | `RetentionCleaner.run()` |
| `heartbeat` | `IntervalTrigger(seconds=30)` | `update_heartbeat()` |

### 6.3 Reconciler pseudocode

```python
class Reconciler:
    POLL_INTERVAL = 2.0  # seconds

    def run_forever(self):
        while not self._stop.is_set():
            cmd = self._pick_command()
            if cmd is not None:
                try:
                    result = self._dispatch(cmd)
                    self._mark_done(cmd.id, result)
                except Exception as e:
                    log.exception("reconciler.failed", cmd_id=cmd.id)
                    self._mark_failed(cmd.id, str(e))
            else:
                time.sleep(self.POLL_INTERVAL)

    def _pick_command(self) -> TriggerCommand | None:
        # SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1
        ...

    def _dispatch(self, cmd) -> dict:
        match cmd.command:
            case "run":        return self._run_sync(cmd)
            case "enable":     return self._enable(cmd)
            case "disable":    return self._disable(cmd)
            case "reload":     return self._reload_config(cmd)
            case _:            raise ValueError(f"unknown command {cmd.command}")
```

### 6.4 Retry policy matrix

| Failure | Retry | Max attempts | Backoff | Post-retry action |
|---|---|---|---|---|
| Haravan 429 | ✅ | 5 | Read `Retry-After` header, fallback exponential 4s→64s | Continue next page |
| Haravan 5xx | ✅ | 3 | Exp 2s→8s | Fail resource, log error |
| Haravan 401/403 | ❌ | — | — | Pause pipeline, Telegram CRITICAL |
| Haravan 4xx khác | ❌ | — | — | Log error, mark resource failed |
| Network timeout | ✅ | 3 | Exp 2s→8s | Fail resource |
| Postgres connection lost | ✅ | 3 | Exp 2s→8s | Fail resource, rollback transaction |
| Unique/FK constraint | ❌ | — | — | Log record ID → `sync_logs`, skip record, continue |
| Transform error (bad type) | ❌ | — | — | Log record, skip, continue |

**💡 Tại sao 401/403 không retry:** Token invalid → retry chỉ tốn quota + spam log; pause ngay + alert cho user fix (creds set) nhanh hơn.

---

## ⑦ Haravan Integration

### 7.1 API basics

- Base URL: `https://{shop}.myharavan.com/admin`
- Auth header: `Authorization: Bearer {token}`
  > ⚠️ **E2E verified 2026-04-24:** Haravan Omnichannel trả `WWW-Authenticate: Bearer`, yêu cầu `Authorization: Bearer`, không phải `X-Haravan-Access-Token`. Mọi code example trong TDD dùng header cũ đều phải dùng header mới này.
- Endpoints MVP:
  - `GET /shop.json` (healthcheck)
  - `GET /orders.json?updated_at_min=...&limit=250&page=N&order=updated_at asc`
  - `GET /orders/count.json` (for validation — optional, phase 2)
  - `GET /customers.json?updated_at_min=...`
  - `GET /products.json?updated_at_min=...` (nested `variants[]` trong payload)
  - `GET /inventory_levels.json?location_ids=...`
  - `GET /locations.json`
- Response header: `X-Haravan-Api-Call-Limit: {used}/80`

### 7.2 HaravanTokenBucket

```python
class HaravanTokenBucket:
    """Client-side leaky bucket đồng bộ với header Haravan."""
    CAPACITY = 80
    LEAK_RATE = 4.0          # tokens / second
    SAFETY_MARGIN = 70       # throttle khi used > 70

    def __init__(self) -> None:
        self._count = 0.0    # tokens đang dùng (không phải tokens còn)
        self._last_leak = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block cho đến khi có quota cho 1 request."""
        with self._lock:
            self._leak()
            while self._count >= self.SAFETY_MARGIN:
                sleep_s = (self._count - self.SAFETY_MARGIN + 1) / self.LEAK_RATE
                self._lock.release()
                time.sleep(max(0.25, sleep_s))
                self._lock.acquire()
                self._leak()
            self._count += 1

    def _leak(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_leak
        self._count = max(0.0, self._count - elapsed * self.LEAK_RATE)
        self._last_leak = now

    def update_from_header(self, header_value: str) -> None:
        """Parse 'used/capacity' (vd '45/80') và override local count."""
        try:
            used, _cap = header_value.split("/", 1)
            with self._lock:
                self._count = float(used)
                self._last_leak = time.monotonic()
        except (ValueError, AttributeError):
            pass  # bad header: ignore, keep local estimate
```

**💡 Tại sao sync từ header:** Client-side local counter drift theo thời gian (clock, concurrent shop admins); header là ground truth → đồng bộ về 0 lỗi.

### 7.3 HaravanClient

> ⚠️ **AMENDED v1.1 → §17.5, §17.7:** `paginate()` page-based bên dưới bị thay bằng **keyset pagination với composite cursor** `(last_updated_at, last_id)` để tránh silent data skip khi source thay đổi mid-pagination. Thêm **cold start prime** gọi `/shop.json` để sync bucket trước request đầu. Header parse phải **robust regex** không crash khi format lạ. Đọc §17.5 & §17.7 trước khi implement.

```python
class HaravanClient:
    def __init__(self, shop_domain: str, access_token: str,
                 bucket: HaravanTokenBucket, http: httpx.Client):
        self._base = f"https://{shop_domain}/admin"
        self._headers = {"Authorization": f"Bearer {access_token}"}
        self._bucket = bucket
        self._http = http

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=64),
        retry=retry_if_exception_type((HaravanRateLimitError, httpx.TimeoutException, Haravan5xxError)),
        reraise=True,
    )
    def get(self, path: str, params: dict | None = None) -> httpx.Response:
        self._bucket.acquire()
        resp = self._http.get(f"{self._base}/{path}", params=params, headers=self._headers)
        if "X-Haravan-Api-Call-Limit" in resp.headers:
            self._bucket.update_from_header(resp.headers["X-Haravan-Api-Call-Limit"])
        self._raise_for_status(resp)
        return resp

    def _raise_for_status(self, resp: httpx.Response) -> None:
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", "4"))
            time.sleep(retry_after)
            raise HaravanRateLimitError(retry_after=retry_after)
        if resp.status_code in (401, 403):
            raise HaravanAuthError(status=resp.status_code)
        if 500 <= resp.status_code < 600:
            raise Haravan5xxError(status=resp.status_code)
        resp.raise_for_status()

    def paginate(self, path: str, params: dict, page_size: int = 250) -> Iterator[list[dict]]:
        """Yield batches of records; stop khi trang cuối."""
        page = 1
        while True:
            resp = self.get(path, params={**params, "page": page, "limit": page_size})
            body = resp.json()
            records = body.get(path.replace(".json", ""), [])   # {"orders": [...]}
            if not records:
                return
            yield records
            if len(records) < page_size:
                return
            page += 1
```

### 7.4 Resource handlers

> ⚠️ **AMENDED v1.1 → §17.5:** `extract_orders` bên dưới dùng page-based + `updated_at_min - 5min overlap` — có 3 bug (page instability khi nhiều order update mid-pagination, tie-breaker thiếu cho records cùng `updated_at`, infinite re-fetch khi bucket chậm). Thay bằng **keyset + composite cursor** `(last_updated_at, last_id)` với sort `order=updated_at asc, id asc`.

Mỗi resource có handler kế thừa `BaseResourceHandler`:

```python
class OrdersHandler(BaseResourceHandler):
    name = "orders"
    endpoint = "orders.json"
    incremental_field = "updated_at"
    table_name = "orders"
    nested_children = [("line_items", OrderLineItemsHandler)]
    supports_incremental = True

    def fetch_incremental(self, client, since: datetime) -> Iterator[list[dict]]:
        params = {
            "updated_at_min": (since - timedelta(minutes=5)).isoformat(),  # 5m overlap
            "order": "updated_at asc",
        }
        yield from client.paginate(self.endpoint, params)
```

**💡 Tại sao handler-per-resource:** Khác biệt nested (products→variants, orders→line_items), sync_mode (inventory_levels full refresh), cursor field (vài resource không có `updated_at`) → tách riêng dễ test từng cái.

---

## ⑧ Data Transformation & Load

### 8.1 Transform pipeline

```
raw record (dict)
    │
    ├─► flatten_top_level   (whitelist columns → DB row dict)
    ├─► flatten_nested      (customer.* → customer_id, customer_email, ...)
    ├─► jsonb_passthrough   (transactions, tax_lines, ... → JSONB)
    ├─► apply_yaml_mapping  (rename, skip, type_override)
    ├─► ensure_raw_column   (_raw = original dict as JSONB)
    └─► validate_types      (Pydantic model → DB row)
```

### 8.2 Upsert pattern

```python
def upsert_batch(conn: Connection, table: Table,
                 rows: list[dict], sync_id: int) -> int:
    if not rows:
        return 0
    stmt = insert(table).values(rows)
    excluded = stmt.excluded
    update_cols = {
        c.name: excluded[c.name]
        for c in table.columns
        if c.name not in ("id", "created_at")     # PK + immutable
    }
    update_cols["_synced_at"] = func.now()
    stmt = stmt.on_conflict_do_update(
        index_elements=[table.c.id],
        set_=update_cols,
    )
    result = conn.execute(stmt)
    return result.rowcount
```

### 8.3 Transaction boundary invariant (⚠️ CRITICAL)

> ⚠️ **AMENDED v1.1 → §17.5, §17.6:** Invariant bên dưới đúng nhưng **chưa đủ** — cần thêm (a) tx dest & internal dùng **snapshot nhất quán** qua `pg_export_snapshot()` cho weekly full, (b) log sink DB phải **async + bounded queue** để không block hot path khi Postgres chậm, (c) `sync_logs` phải **partitioned by RANGE(timestamp)** để cleanup O(1). Đọc §17.6 trước khi implement logger.

```python
# INVARIANT (NEVER VIOLATE):
#   destination commit HAPPENS BEFORE internal checkpoint commit.
#   Crash in between → next sync re-processes same records → safe because upsert is idempotent.
#   NEVER the opposite order (would lose records on crash).

def run_resource(self, spec: SyncSpec) -> SyncResult:
    batches = self.client.paginate(...)
    max_updated = spec.checkpoint_ts
    total = 0

    for batch in batches:
        transformed = [self.transformer(r) for r in batch]
        with self.destination_engine.begin() as dest_conn:   # (1) commit data
            upsert_batch(dest_conn, self.table, transformed, sync_id=spec.sync_id)
        max_updated = max(max_updated, max(r["updated_at"] for r in transformed))
        total += len(transformed)

    # (2) commit checkpoint ONLY after all batches succeeded
    with self.internal_engine.begin() as int_conn:
        save_checkpoint(int_conn, spec.pipeline, spec.resource, max_updated, spec.sync_id)

    return SyncResult(records=total, high_watermark=max_updated)
```

**💡 Tại sao checkpoint sau:** Nếu crash giữa (1) và (2), checkpoint vẫn là lần trước → lần sync tới re-fetch batch đã upsert (idempotent) → **zero data loss**. Ngược lại: commit checkpoint trước rồi crash → records chưa upsert bị bỏ qua **vĩnh viễn** → **data loss**.

### 8.4 High-watermark from batch (không dùng `now()`)

> ⚠️ **AMENDED v1.1 → §17.5:** `new_watermark = max(r["updated_at"] for r in transformed if r.get("updated_at"))` raise `ValueError` khi batch rỗng/toàn `None` → sync crash. Thay bằng `compute_watermark()` return `None` nếu không có record hợp lệ → caller skip save checkpoint giữ nguyên cũ. Checkpoint chuyển sang **composite** `(last_updated_at, last_id)` để tránh re-fetch records cùng timestamp.

```python
# ❌ WRONG: dùng time.now() có thể nhảy tới tương lai vs Haravan clock
# checkpoint = datetime.now()

# ✅ RIGHT: lấy max updated_at từ records thật
# checkpoint = max(r["updated_at"] for r in batch)
#
# Lần sync sau: updated_at_min = checkpoint - 5 phút (overlap buffer)
```

**💡 Tại sao:** Clock VPS có thể drift so với Haravan; nếu `now()` tương lai hơn Haravan → record có `updated_at ≤ now()` nhưng > haravan_now bị skip → lost. Watermark-from-batch an toàn.

### 8.5 Weekly Full Refresh + soft delete

> ⚠️ **AMENDED v1.1 → §17.1:** Code bên dưới dùng `seen_ids: set[int]` để soft-delete KHÔNG scalable (1M records = ~8MB set in RAM + query `notin_(seen_ids)` làm planner chết) và KHÔNG crash-safe (crash giữa chừng → mất set → stale records vĩnh viễn không bị soft-delete). Thay bằng **`_sync_id` marker pattern**: mỗi full refresh sinh UUID, mark record đã thấy, soft-delete record còn marker cũ. Crash → resume được.

```python
def run_weekly_full_refresh(self, pipeline: str) -> None:
    for resource in self.enabled_resources(pipeline):
        sync_start = datetime.utcnow()
        # 1. Fetch tất cả records, upsert (_synced_at = now())
        self.run_resource(spec=SyncSpec(mode="full_refresh", ...))

        # 2. Mark deleted: records không được touch trong lần full refresh này
        with self.destination_engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE {resource} SET _deleted_at = :now
                WHERE _synced_at < :sync_start AND _deleted_at IS NULL
            """), {"now": datetime.utcnow(), "sync_start": sync_start})
```

### 8.6 Validation (F5)

```python
def validate_resource(self, sync_result: SyncResult) -> list[ValidationResult]:
    results = []

    # Rule 1: row_count_delta
    current = conn.scalar(select(func.count()).select_from(table))
    previous = conn.scalar(
        select(ValidationResult.details["count"].astext.cast(Integer))
        .where(ValidationResult.resource == resource, ValidationResult.rule == "row_count_delta")
        .order_by(ValidationResult.id.desc())
        .limit(1)
    )
    if previous is not None and not is_weekly_full_refresh:
        delta_pct = (previous - current) / previous * 100
        status = "warning" if delta_pct > 10 else "passed"
        results.append(ValidationResult(rule="row_count_delta",
                                         status=status,
                                         details={"current": current, "previous": previous, "delta_pct": delta_pct}))

    # Rule 2: required_fields_not_null
    nulls = conn.execute(
        text(f"SELECT id FROM {resource} WHERE id IS NULL OR created_at IS NULL LIMIT 10")
    ).fetchall()
    status = "failed" if nulls else "passed"
    results.append(ValidationResult(rule="required_fields_not_null",
                                     status=status,
                                     details={"null_rows": [r.id for r in nulls]}))

    return results
```

---

## ⑨ Telegram Notification

### 9.1 Setup flow

```
1. User → @BotFather → /newbot → receive bot_token
2. User → /start bot_username (any message)
3. User: curl https://api.telegram.org/bot{token}/getUpdates | jq '.result[0].message.chat.id'
4. Edit /etc/flowbyte/config.yml:
   alerting:
     telegram:
       bot_token: "123456:ABC..."
       chat_id: "987654321"
5. flowbyte alert test   → "🟢 Flowbyte alert channel OK"
```

### 9.2 Triggers MVP

| # | Event | Severity | Message template |
|---|---|---|---|
| 1 | Sync resource fail sau 3 retry | 🔴 CRITICAL | `"🔴 {pipeline}.{resource} FAILED\nError: {error_msg}\nRun: flowbyte logs {pipeline} --errors"` |
| 2 | Scheduler heartbeat stale > 2h | 🔴 CRITICAL | `"🔴 Flowbyte scheduler DEAD\nLast beat: {last_beat_ago}\nSSH VPS + docker logs flowbyte"` |

### 9.3 Dedup + rate limit

```python
class TelegramAlerter:
    DEDUP_WINDOW = timedelta(minutes=5)
    MAX_PER_HOUR_PER_PIPELINE = 3

    def send(self, event: AlertEvent) -> None:
        # Dedup
        fingerprint = f"{event.pipeline}:{event.resource}:{event.error_class}"
        last_sent = self._last_sent.get(fingerprint)
        if last_sent and (now() - last_sent) < self.DEDUP_WINDOW:
            log.info("alert.deduped", fingerprint=fingerprint)
            return

        # Rate limit
        recent = self._sent_within(timedelta(hours=1), pipeline=event.pipeline)
        if len(recent) >= self.MAX_PER_HOUR_PER_PIPELINE:
            log.warning("alert.rate_limited", pipeline=event.pipeline)
            return

        self._post_telegram(event.render())
        self._record_sent(fingerprint)
```

**💡 Tại sao dedup 5 phút + 3/giờ:** Chống spam khi Haravan down 1h → không flood user; vẫn đảm bảo message đầu tiên đến trong < 30s.

---

## ⑩ Security

### 10.1 Authentication & authorization

- **MVP:** No Flowbyte user auth — rely on **Linux file perm** (CLI chạy qua SSH đã auth) + **Docker exec** (root trong container).
- Postgres auth: user role `flowbyte` với `pg_hba.conf` scram-sha-256, password trong env `FLOWBYTE_DB_URL`.
- Haravan: private app access token, không OAuth.

### 10.2 Data protection at rest

| Asset | Protection |
|---|---|
| Master key `/etc/flowbyte/master.key` | chmod 600, mount `:ro` vào container, user tự backup ngoài VPS |
| `credentials` table | AES-256-GCM ciphertext + AAD ref binding |
| YAML configs | Không chứa plaintext secret (chỉ `credentials_ref`) |
| Logs | `access_token` mask `****{last4}` |
| pg_dump backups | User chịu trách nhiệm encrypt khi backup ngoài VPS |

### 10.3 Data in transit

- HTTPS only: `httpx` verify=True mặc định.
- Telegram API: HTTPS.
- Postgres: localhost → plaintext OK; remote → require SSL mode prefer.

### 10.4 SQL injection prevention

- **Tuyệt đối không string-format SQL.** Dùng SQLAlchemy `text(":bindparam")` hoặc `insert().values()`.
- Resource name → whitelist trong `HARAVAN_RESOURCES` set, validate trước khi đưa vào table name.

### 10.5 Backup & disaster recovery

> ⚠️ **AMENDED v1.1 → §17.10:** Bảng bên dưới placeholder "user tự làm" là SPOF lớn: (a) master key mất = **tất cả** credentials mất vĩnh viễn, không recovery, (b) backup script chưa viết, DR chưa từng test. Thay bằng **built-in `flowbyte backup/restore/verify-backup`** với GFS retention (7d/4w/6m), monthly DR drill, master key escrow + rotation. Đọc §17.10 trước khi go-live.

| Scenario | Recovery |
|---|---|
| Master key mất | Credentials không decrypt được → user `flowbyte creds set` lại cho mọi ref |
| Postgres DB corrupt | pg_restore từ cronjob backup user tự làm |
| Container corrupt | `docker compose up -d --force-recreate flowbyte` → auto-migrate + scheduler khôi phục |
| Config YAML mất | Restore từ Git (YAML được recommend commit) |

---

## ⑪ Non-Functional Requirements

### 11.1 Performance targets

| Metric | Target | Đo bằng |
|---|---|---|
| CLI response (non-run) | < 1s | time wrapper |
| `validate` | < 5s | time wrapper |
| Sync 10k records | < 60s | sync_logs.duration_ms |
| Full refresh 76k orders | < 3 min | sync_logs.duration_ms |
| Validation per resource | < 5s | validation_results.duration |
| Telegram alert delivery | < 30s | timestamp log |
| `status` command | < 1s | time wrapper |

### 11.2 Memory budget

| Component | Budget | Note |
|---|---|---|
| Python baseline (interpreter + deps) | ~150 MB | |
| APScheduler + jobstore cache | ~50 MB | |
| SQLAlchemy pool (size=10) | ~100 MB | |
| httpx + connection pool | ~30 MB | |
| Streaming batch buffer (250 records × ~5KB) | ~2 MB/resource | stream processing, không load all |
| structlog queues | ~20 MB | |
| **Headroom** | ~1 GB | Cho spike + fragmentation |
| **Docker limit** | **1.5 GB** | Hard limit |

### 11.3 Concurrency model

- **1 daemon process**, **1 scheduler thread**, **1 worker thread** (APScheduler `max_workers=1`).
- **Tuần tự qua 1 HaravanTokenBucket** cho mọi resource → không race rate limit.
- **Postgres pool size 10** → đủ cho daemon + CLI ad-hoc song song.

### 11.4 Reliability

| Target | Giá trị | Verify |
|---|---|---|
| Scheduler uptime | ≥ 99% | `systemctl`/container uptime + heartbeat stale ratio |
| Sync success rate (tháng 1) | ≥ 99% | `sync_logs`: `SUM(status='success')/COUNT(*)` |
| Data freshness (orders) | < 2h 15min | `now() - MAX(sync_checkpoints.checkpoint_ts)` |

### 11.5 Scalability ceiling (MVP)

- 1 shop, 1 pipeline, 6 resources.
- ~200 orders/day → ~8k records sync/ngày → well within budget.
- Out-of-scope: > 1 shop (multi-shop), > 100k records/sync (cần chunking nâng cao), > 10 pipelines (cần scheduler pool lớn hơn).

---

## ⑫ Testing Strategy

### 12.1 Test pyramid (Hybrid 2-tier)

```
                 ┌─────────────────────────┐
                 │    Manual checklist      │  < 1% effort, trước release
                 │   (deployment smoke)     │
                 └─────────────────────────┘
              ┌─────────────────────────────────┐
              │    Integration (testcontainers   │  ~25% effort
              │    + respx + freezegun)          │
              └─────────────────────────────────┘
        ┌──────────────────────────────────────────────┐
        │    Unit (pure functions, mocks-free)          │  ~75% effort, TDD vòng ngắn
        └──────────────────────────────────────────────┘
```

### 12.2 Unit tests (tier 1)

- **Scope:** Pure functions: transform, token bucket, cron parse, YAML parse, encryption roundtrip.
- **Rule:** Không DB, không HTTP, không filesystem (trừ `tmp_path`).
- **Tốc độ:** Full unit suite < 5s.
- **Ví dụ:**
  - `test_token_bucket_refills_at_4rps`
  - `test_transform_renames_total_price_to_revenue`
  - `test_aesgcm_roundtrip_with_correct_aad`
  - `test_aesgcm_fails_with_wrong_aad`
  - `test_flatten_customer_nested_to_top_level_cols`

### 12.3 Integration tests (tier 2)

- **Scope:** End-to-end 1 resource sync với Postgres thật + Haravan mock.
- **Fixtures:**
  - `postgres_container` (module-scoped testcontainers)
  - `internal_engine`, `destination_engine` (function-scoped, migrate head)
  - `haravan_respx` (mock /orders.json với fixtures JSON)
  - `frozen_time` (freezegun 2026-01-01 00:00:00 UTC)
- **Ví dụ:**
  - `test_incremental_sync_idempotent_on_crash`: chạy sync 2 lần cùng data → row count giống nhau
  - `test_checkpoint_not_advanced_when_destination_commit_fails`: simulate rollback → checkpoint giữ cũ
  - `test_429_retry_after_header_respected`: respx trả 429 + Retry-After=2 → đợi 2s rồi retry
  - `test_weekly_full_refresh_marks_deleted`: records vắng mặt → `_deleted_at` set
  - `test_telegram_dedup_within_5min`: gửi 2 alert same fingerprint → chỉ 1 API call

### 12.4 Coverage target

- Unit: **≥ 85%** branch coverage.
- Integration: path coverage cho critical flows (checkpoint, retry, upsert).
- Tổng: **≥ 80% line coverage** (enforced trong CI).

### 12.5 CI pipeline

```yaml
# .github/workflows/ci.yml (sketch)
jobs:
  lint:       ruff check . && ruff format --check .
  typecheck:  mypy src/
  unit:       pytest tests/unit/ -n auto --cov=flowbyte --cov-fail-under=85
  integration: pytest tests/integration/ -n 2 (requires Docker)
```

### 12.6 Manual smoke checklist (pre-release)

- [ ] Fresh install trên VPS staging: `docker compose up -d flowbyte`
- [ ] `flowbyte init-master-key` → backup file, xác nhận chmod 600
- [ ] `flowbyte init shop_test` → edit YAML
- [ ] `flowbyte creds set` → `flowbyte validate` pass
- [ ] `flowbyte run shop_test` → `flowbyte status` 🟢
- [ ] Thử stop container khi đang sync → restart → `sync_logs` không corrupt
- [ ] `flowbyte alert test` → nhận Telegram < 30s
- [ ] Chạy 48h liên tục, `sync_logs` không có error rate > 1%

---

## ⑬ Coding Conventions

### 13.1 Project structure

```
flowbyte/
├── src/flowbyte/
│   ├── __init__.py
│   ├── daemon.py              # Entry point cho daemon-start
│   ├── cli/
│   │   ├── __init__.py
│   │   ├── app.py             # Typer app
│   │   ├── creds.py
│   │   ├── pipelines.py
│   │   ├── status.py
│   │   └── _format.py         # Rich helpers
│   ├── config/
│   │   ├── models.py          # Pydantic models
│   │   ├── loader.py          # YAML + env overlay
│   │   └── defaults.py
│   ├── db/
│   │   ├── engines.py         # internal_engine, destination_engine
│   │   ├── schema_internal.py # SQLAlchemy Table definitions
│   │   ├── schema_destination.py
│   │   └── migrations/        # Alembic
│   ├── haravan/
│   │   ├── client.py          # HaravanClient
│   │   ├── token_bucket.py
│   │   ├── exceptions.py
│   │   └── resources/
│   │       ├── base.py
│   │       ├── orders.py
│   │       ├── customers.py
│   │       └── ...
│   ├── sync/
│   │   ├── runner.py          # SyncRunner
│   │   ├── transform.py
│   │   ├── loader.py          # upsert_batch, save_checkpoint
│   │   └── validation.py
│   ├── scheduling/
│   │   ├── scheduler.py       # APScheduler wrapper
│   │   └── reconciler.py
│   ├── alerting/
│   │   ├── telegram.py
│   │   └── dedup.py
│   ├── security/
│   │   ├── crypto.py          # AES-256-GCM wrappers
│   │   └── master_key.py
│   ├── logging/
│   │   └── setup.py           # structlog configure
│   └── bootstrap.py           # init_db, ensure_master_key
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── fixtures/
│   │   └── haravan/           # sample JSON responses
│   └── conftest.py
├── alembic.ini
├── pyproject.toml
├── Dockerfile
├── docker-compose.yml
└── README.md
```

### 13.2 Naming conventions

| Element | Convention | Ví dụ |
|---|---|---|
| Module | `snake_case.py` | `token_bucket.py` |
| Class | `PascalCase` | `HaravanClient` |
| Function | `snake_case()` | `upsert_batch()` |
| Constant | `UPPER_SNAKE` | `DEFAULT_BATCH_SIZE = 250` |
| Private | `_leading_underscore` | `_leak()`, `_lock` |
| SQL table | `snake_case` singular-ish | `orders`, `sync_logs` |
| SQL column | `snake_case` | `updated_at`, `total_price` |
| Internal SQL prefix | `_` prefix cho metadata | `_raw`, `_deleted_at`, `_synced_at` |
| Test function | `test_<unit>_<scenario>` | `test_token_bucket_refills_at_4rps` |

### 13.3 Type hints (strict)

- **Every function signature** có type hint cho args + return.
- **No `Any`** trừ khi unavoidable (comment lý do).
- **`Optional[X]` → `X | None`** (PEP 604 syntax, Python 3.10+).
- Pydantic models cho mọi boundary (YAML, API response, config).

```python
# ✅ Good
def fetch_since(client: HaravanClient, since: datetime) -> Iterator[list[dict[str, Any]]]: ...

# ❌ Bad
def fetch_since(client, since): ...
```

### 13.4 Error handling

```python
# flowbyte/haravan/exceptions.py
class FlowbyteError(Exception): """Base."""
class HaravanError(FlowbyteError): """Any Haravan API error."""
class HaravanAuthError(HaravanError): """401/403. Never retry."""
class HaravanRateLimitError(HaravanError):
    def __init__(self, retry_after: float): self.retry_after = retry_after
class Haravan5xxError(HaravanError): ...
class ConfigError(FlowbyteError): """YAML invalid."""
class CryptoError(FlowbyteError): """Decrypt failed."""
```

- Catch specific, raise specific. **Không** `except Exception` trừ top-level daemon loop.
- Log bằng `log.exception()` (có stack) chứ không `log.error(str(e))`.

### 13.5 SQL/DB patterns

```python
# ✅ Good: SQLAlchemy Core insert().on_conflict_do_update()
stmt = insert(orders).values(rows).on_conflict_do_update(...)

# ✅ Good: text() with bind params
conn.execute(text("UPDATE orders SET _deleted_at = :t WHERE id = :id"),
             {"t": now, "id": oid})

# ❌ Bad: f-string SQL
conn.execute(f"UPDATE orders SET x = {value}")
```

**Transaction scope:**
```python
# ✅ Good: explicit begin(), ONE logical unit per block
with engine.begin() as conn:
    conn.execute(...)
    conn.execute(...)   # commits OR rollbacks together
```

### 13.6 Logging patterns

```python
# setup
log = structlog.get_logger()

# bind context
log = log.bind(pipeline=pipeline, resource=resource, sync_id=sync_id)

# events
log.info("sync.started", mode=mode, checkpoint=checkpoint)
log.info("sync.batch_loaded", records=len(batch), page=page)
log.exception("sync.failed", error_class=e.__class__.__name__)  # auto include stack
```

Rules:
- **Event name `noun.verb`** (past tense): `sync.started`, `creds.rotated`.
- **Never interpolate vào message**; mọi biến qua kwargs.
- **Mask secrets** ở `processors` stage, không dựa vào caller nhớ.

### 13.7 Testing patterns

```python
# ✅ Arrange-Act-Assert blocks, no comments needed
def test_token_bucket_sync_from_header():
    bucket = HaravanTokenBucket()
    bucket.update_from_header("45/80")

    assert bucket._count == 45.0

# ✅ Parametrize instead of loops
@pytest.mark.parametrize("header,expected", [
    ("45/80", 45.0),
    ("0/80", 0.0),
    ("80/80", 80.0),
])
def test_token_bucket_parses_various_headers(header, expected):
    bucket = HaravanTokenBucket()
    bucket.update_from_header(header)
    assert bucket._count == expected
```

### 13.8 Tool config

**`pyproject.toml`** relevant sections:

```toml
[tool.ruff]
line-length = 100
target-version = "py311"
select = ["E", "F", "I", "B", "UP", "SIM", "RUF"]
ignore = ["E501"]  # handled by formatter

[tool.mypy]
python_version = "3.11"
strict = true
plugins = ["pydantic.mypy"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra --strict-markers --strict-config"
markers = [
    "integration: requires Docker (testcontainers)",
]

[tool.coverage.run]
branch = true
source = ["src/flowbyte"]

[tool.coverage.report]
fail_under = 80
exclude_lines = ["if TYPE_CHECKING:", "raise NotImplementedError"]
```

### 13.9 Commit conventions

Conventional Commits style:

```
<type>(<scope>): <short description>

<body if needed>

<footer>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`.
Scopes: `haravan`, `sync`, `cli`, `scheduler`, `alerting`, `db`, `config`.

Ví dụ:
```
feat(haravan): sync token bucket from X-Haravan-Api-Call-Limit header
fix(sync): never advance checkpoint before destination commit
```

---

## ⑭ Environment Variables

| Var | Default | Mô tả |
|---|---|---|
| `FLOWBYTE_DB_URL` | (required) | `postgresql+psycopg://user:pw@host:5432/flowbyte_internal` |
| `FLOWBYTE_DEST_DB_URL` | same host, db=`flowbyte_destination` | Destination override |
| `FLOWBYTE_MASTER_KEY_PATH` | `/etc/flowbyte/master.key` | Path to 32-byte key |
| `FLOWBYTE_CONFIG_DIR` | `/etc/flowbyte` | Root for `config.yml` + `pipelines/*.yml` |
| `FLOWBYTE_LOG_LEVEL` | `INFO` | `DEBUG`/`INFO`/`WARN`/`ERROR` |
| `FLOWBYTE_LOG_FORMAT` | `json` | `json`/`console` |
| `FLOWBYTE_LOG_FILE` | `/var/log/flowbyte/flowbyte.log` | Rotating file path |
| `FLOWBYTE_TZ` | `UTC` | Timezone for scheduler + display |
| `FLOWBYTE_VERSION` | (set at build) | Shown in heartbeat + logs |

---

## ⑮ Risks & Trade-offs

| Risk | Impact | Mitigation |
|---|---|---|
| Haravan API breaking change | High — sync fail | `_raw JSONB` preserves unknown fields; Pydantic non-strict on unknowns; monitor release notes |
| Master key lost | Critical — credentials unrecoverable | CLI forces backup prompt on init; doc recovery in README |
| Clock drift VPS vs Haravan | Medium — incremental miss records | High-watermark-from-batch + 5min overlap |
| APScheduler thread deadlock | High — scheduler dead silently | Heartbeat 30s + 2h stale alert |
| Postgres disk full | High — sync fail | Retention 90d/30d + daily cleanup 3am |
| n8n hogs VPS RAM | Medium — Flowbyte OOM killed | Docker memory limit 1.5GB isolate |
| DB polling latency for CLI | Low — CLI feels slow | 2s poll interval acceptable; future: LISTEN/NOTIFY |
| Single worker thread bottleneck | Low at MVP scale | Future: per-shop worker (phase 3 multi-shop) |
| Haravan 401 not caught early | Medium — silent data gap | Validate on every schedule load + Telegram CRITICAL on 401 |
| Transform YAML silently wrong | Medium — wrong data in DB | `flowbyte validate` dry-run; unit tests per mapping |

---

## ⑯ Implementation Plan

### Phase 1a — Foundation (Tuần 1-2)

**Deliverables:**
- [ ] Repo skeleton: `pyproject.toml`, ruff + mypy + pytest config
- [ ] Module scaffolds theo §13.1
- [ ] `flowbyte/security/crypto.py` + unit tests (AES-256-GCM roundtrip, AAD mismatch)
- [ ] `flowbyte/config/models.py` (Pydantic v2 schemas cho pipeline YAML + global config)
- [ ] `flowbyte/db/schema_internal.py` + Alembic initial migration
- [ ] `flowbyte/logging/setup.py` (structlog 3 sinks)
- [ ] CI: lint + typecheck + unit test gate

**Exit criteria:** `pytest tests/unit/` green, coverage ≥ 85% on touched modules.

### Phase 1b — Haravan client (Tuần 2-3)

**Deliverables:**
- [ ] `HaravanTokenBucket` + unit tests (refill, header sync, concurrency)
- [ ] `HaravanClient.get` với tenacity retry + unit tests (429, 401, 5xx, timeout)
- [ ] `HaravanClient.paginate`
- [ ] `OrdersHandler` as first concrete resource + fixtures JSON
- [ ] Integration test: mock /orders.json với respx, assert pagination stops đúng

**Exit criteria:** Có thể `HaravanClient(...).paginate("orders.json", {...})` trả records từ mock.

### Phase 2a — Sync engine + checkpoint (Tuần 3-4)

**Deliverables:**
- [ ] `transform.flatten_record` + unit tests
- [ ] `loader.upsert_batch` integration test (testcontainers postgres)
- [ ] `SyncRunner.run_resource` với transaction boundary invariant
- [ ] `save_checkpoint` atomic
- [ ] Integration test: `test_incremental_sync_idempotent_on_crash`
- [ ] Integration test: `test_checkpoint_not_advanced_when_destination_commit_fails`

**Exit criteria:** Sync `orders` từ mock Haravan → Postgres testcontainer thành công, re-run không duplicate.

### Phase 2b — 5 remaining resources + Weekly Full (Tuần 4-5)

**Deliverables:**
- [ ] `customers`, `products` (+ nested `variants`), `inventory_levels`, `locations` handlers
- [ ] Weekly Full Refresh + soft delete logic + test
- [ ] Validation rules (row_count_delta, required_fields_not_null)

### Phase 3 — Scheduler + CLI + Alerting (Tuần 5-6)

**Deliverables:**
- [ ] APScheduler jobstore setup
- [ ] Reconciler loop + `trigger_commands` table
- [ ] All CLI commands theo §4.1
- [ ] `TelegramAlerter` với dedup + rate limit
- [ ] Heartbeat + stale detection

**Exit criteria:** `flowbyte run shop_main` end-to-end via docker exec, Telegram test nhận message < 30s.

### Phase 4 — Packaging + Deployment (Tuần 6-7)

**Deliverables:**
- [ ] Dockerfile multi-stage (builder + slim runtime)
- [ ] `docker-compose.yml` (share network với n8n)
- [ ] Entry point `daemon-start`: run Alembic upgrade + launch scheduler
- [ ] README: deployment checklist (clone PRD §⑫)
- [ ] Manual smoke test trên VPS staging 48h

**Exit criteria:** Deploy checklist xanh toàn bộ; sync 48h không error rate > 1%.

---

## 17. Review v1.1 — Amendments (Senior Review 2026-04-24)

> Section này tổng hợp 11 vấn đề phát hiện trong phiên senior review và phương án tối ưu đã chốt. Mỗi subsection **override** code/schema tương ứng ở section gốc.

### 17.1 Weekly Full Refresh — `_sync_id` marker thay `seen_ids` set

**Vấn đề (override §8.5):**
- `seen_ids: set[int]` không scalable: 1M records ≈ 8MB RAM + planner PostgreSQL chết khi `WHERE id NOT IN (seen_ids)`.
- Không crash-safe: daemon restart giữa chừng → set mất → stale records không bao giờ bị soft-delete.

**Phương án chốt — `_sync_id` marker:**

Schema delta (mọi destination table):
```sql
ALTER TABLE orders ADD COLUMN _sync_id uuid;
ALTER TABLE orders ADD COLUMN _synced_at timestamptz;
CREATE INDEX ix_orders_sync_id ON orders(_sync_id) WHERE deleted_at IS NULL;
```

Flow:
```python
def run_weekly_full_refresh(self, pipeline: str, resource: str) -> None:
    sync_id = uuid.uuid4()  # marker cho run này
    started_at = datetime.now(tz=self.tz)

    # 1. Upsert mọi record fetch được, gắn marker sync_id hiện tại
    for batch in client.paginate_full(resource):
        upsert_batch(batch, extra_cols={"_sync_id": sync_id, "_synced_at": started_at})

    # 2. Soft-delete mọi record còn marker CŨ (không được chạm trong run này)
    conn.execute(text("""
        UPDATE orders
        SET deleted_at = :now, _sync_reason = 'not_in_source'
        WHERE (_sync_id IS NULL OR _sync_id != :sid)
          AND deleted_at IS NULL
    """), {"now": datetime.now(tz=self.tz), "sid": sync_id})

    # 3. Validate: soft_delete_sanity (xem §17.8) — fail nếu >5% bị delete
```

**Trade-off:** Thêm 2 column (+16 bytes/row) và 1 index; đổi lại scalable tới 10M+ records và resume-able sau crash.

---

### 17.2 Bootstrap: service riêng + non-root UID

**Vấn đề:** §2.4 nói bind-mount `/config:ro` nhưng `flowbyte creds set` phải ghi vào `credentials.enc` → mâu thuẫn. Docker user mặc định root → master key file chmod 600 bị bypass.

**Phương án chốt:**

`docker-compose.yml`:
```yaml
services:
  flowbyte-daemon:
    image: flowbyte:1.1
    user: "1000:1000"                    # non-root
    volumes:
      - ./config:/config:ro              # read-only cho runtime
      - ./data:/data                     # master key + SQLite (nếu dùng)
    depends_on: [postgres]
    command: ["flowbyte", "start"]

  flowbyte-admin:                        # service riêng cho set creds
    image: flowbyte:1.1
    user: "1000:1000"
    volumes:
      - ./config:/config                 # RW, chỉ khi admin
      - ./data:/data
    profiles: ["admin"]                  # không start mặc định
    entrypoint: ["flowbyte"]

# Usage:
# docker compose run --rm flowbyte-admin creds set shop_main ...
```

`Dockerfile`:
```dockerfile
RUN groupadd -g 1000 flowbyte && useradd -u 1000 -g 1000 -m flowbyte
USER flowbyte
```

---

### 17.3 Timezone config toàn cục (ZoneInfo)

**Vấn đề:** Crontab `"0 2 * * 0"` mặc định UTC = 9 AM giờ VN = peak → scheduler full refresh đụng giờ vàng shop. DST không áp dụng với VN nhưng nếu mở rộng ra customer quốc tế sẽ sai.

**Phương án chốt:**

`config/pipelines.yaml`:
```yaml
global:
  timezone: "Asia/Ho_Chi_Minh"           # toàn bộ cron eval ở timezone này

pipelines:
  shop_main:
    resources:
      orders:
        schedule: "*/5 * * * *"          # mỗi 5 phút, TZ=config
      products:
        schedule: "0 */2 * * *"
    weekly_full:
      schedule: "0 3 * * 0"              # 3 AM Chủ nhật giờ VN = off-peak thật
```

Implementation:
```python
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler

tz = ZoneInfo(config.global_.timezone)   # fail-fast nếu TZ invalid
scheduler = BlockingScheduler(timezone=tz)
scheduler.add_job(..., trigger=CronTrigger.from_crontab(schedule, timezone=tz))
```

Mọi `datetime` trong code dùng `datetime.now(tz=tz)`, lưu DB ở `timestamptz`.

---

### 17.4 Reconciler Step 0 — Recovery stale `claimed` requests

**Vấn đề:** `sync_requests.status = 'claimed'` không tự unstuck khi daemon crash → requests đó mất vĩnh viễn.

**Phương án chốt:**

Schema delta:
```sql
ALTER TABLE sync_requests ADD COLUMN claimed_at timestamptz;
ALTER TABLE sync_requests ADD COLUMN claimed_by_pid integer;
ALTER TABLE sync_requests ADD COLUMN recovery_count integer DEFAULT 0;
```

Reconciler loop:
```python
class Reconciler:
    POLL_INTERVAL = 2.0
    STALE_CLAIM_SEC = 300   # 5 phút

    def tick(self):
        # Step 0: Recovery — release stale claims
        conn.execute(text("""
            UPDATE sync_requests
            SET status='pending',
                recovery_count = recovery_count + 1,
                claimed_at=NULL, claimed_by_pid=NULL
            WHERE status='claimed'
              AND claimed_at < :threshold
              AND recovery_count < 3
        """), {"threshold": datetime.now(tz=self.tz) - timedelta(seconds=self.STALE_CLAIM_SEC)})

        # Mark thrashing requests (recovery >= 3) as failed → alert
        conn.execute(text("""
            UPDATE sync_requests
            SET status='failed', error='recovery_limit_exceeded'
            WHERE status='claimed' AND recovery_count >= 3
        """))

        # Step 1-N: existing logic (claim pending, run, mark done/failed)
        ...
```

Thrashing alert → Telegram.

---

### 17.5 Keyset pagination + composite cursor

**Vấn đề (override §7.3, §7.4, §8.4):**
- Page-based `?page=N` instability: nếu records update mid-pagination, page shift → skip/dup records.
- `updated_at_min - 5min overlap` không fix được: tie-breaker thiếu khi nhiều record cùng `updated_at`.
- `compute_watermark()` crash nếu batch rỗng/toàn `None`.

**Phương án chốt — Keyset:**

Haravan call:
```
GET /admin/orders.json
  ?updated_at_min={last_ts}
  &id_min={last_id}              # tie-breaker
  &order=updated_at asc, id asc  # deterministic
  &limit=250
```

Cursor composite:
```python
@dataclass
class Cursor:
    last_updated_at: datetime | None
    last_id: int | None

def compute_watermark(records: list[dict]) -> Cursor | None:
    valid = [r for r in records if r.get("updated_at") and r.get("id")]
    if not valid:
        return None   # caller: giữ cursor cũ, KHÔNG save
    last = max(valid, key=lambda r: (r["updated_at"], r["id"]))
    return Cursor(last["updated_at"], last["id"])
```

Regression guard:
```python
if new_cursor and old_cursor:
    if (new_cursor.last_updated_at, new_cursor.last_id) < (old_cursor.last_updated_at, old_cursor.last_id):
        raise CheckpointRegressionError(...)   # refuse save, alert
```

Checkpoint schema:
```sql
ALTER TABLE sync_checkpoints ADD COLUMN last_id bigint;
-- last_updated_at đã có
```

---

### 17.6 Async bounded log sink + partitioned `sync_logs`

**Vấn đề (override §8.3 phần logging):** DB sink sync → block hot path nếu Postgres chậm. Cleanup `DELETE FROM sync_logs WHERE ts < ...` quét table → chậm khi table lớn.

**Phương án chốt:**

Async sink:
```python
class AsyncDBSink:
    def __init__(self, engine, queue_size=10000):
        self.q = queue.Queue(maxsize=queue_size)
        self.dropped = 0
        threading.Thread(target=self._drain, daemon=True).start()

    def write(self, record):
        try:
            self.q.put_nowait(self._redact(record))
        except queue.Full:
            self.dropped += 1   # metric: flowbyte_log_drops_total

    def _drain(self):
        batch = []
        while True:
            batch.append(self.q.get())
            if len(batch) >= 100 or self.q.empty():
                conn.execute(sync_logs.insert(), batch)
                batch.clear()

    def _redact(self, rec):
        # deep redact: access_token, master_key, card_number, v.v.
        return deep_redact(rec, PII_KEYS)
```

Partitioned table:
```sql
CREATE TABLE sync_logs (
    id bigserial,
    ts timestamptz NOT NULL,
    level text, msg text, context jsonb
) PARTITION BY RANGE (ts);

-- Monthly partitions, maintained by cron:
CREATE TABLE sync_logs_2026_04 PARTITION OF sync_logs
  FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

-- Cleanup = DROP PARTITION (O(1) thay vì DELETE)
DROP TABLE sync_logs_2025_10;
```

---

### 17.7 Schedule stagger + cold start prime + robust header regex + misfire

**Vấn đề (override §6.2, §7.3):**
- Cold start → tất cả jobs `*/5 * * * *` fire cùng phút → bucket bão hòa.
- Cold start lần đầu: bucket local = 0, request đầu tiên 429.
- Header regex hardcode `r"(\d+)/(\d+)"` crash khi Haravan đổi format.
- APScheduler misfire event không được xử lý → silent skip.

**Phương án chốt:**

Stagger mặc định:
```yaml
pipelines:
  shop_main:
    schedule_jitter_sec: 30   # global; từng resource offset thêm
    resources:
      orders:    { schedule: "*/5 * * * *", jitter_offset_sec: 0  }
      products:  { schedule: "*/5 * * * *", jitter_offset_sec: 15 }
      customers: { schedule: "*/5 * * * *", jitter_offset_sec: 30 }
```

Cold start prime:
```python
def _prime_bucket(self):
    # Gọi /admin/shop.json lấy header rate limit → seed bucket
    resp = self.http.get(f"https://{self.shop}/admin/shop.json",
                         headers={"Authorization": f"Bearer {self.token}"})
    self.bucket.sync_from_header(resp.headers.get("X-Haravan-Api-Call-Limit"))
```

Robust regex + bounds check:
```python
RATE_LIMIT_RE = re.compile(r"^\s*(\d{1,4})\s*/\s*(\d{1,4})\s*$")

def sync_from_header(self, header: str | None):
    if not header:
        return
    m = RATE_LIMIT_RE.match(header)
    if not m:
        log.warning("rate_limit_header_malformed", raw=header)
        return   # fallback: dùng local counter
    used, limit = int(m.group(1)), int(m.group(2))
    if not (0 <= used <= limit <= 10000):
        log.warning("rate_limit_out_of_range", used=used, limit=limit)
        return
    self.used = used
    self.limit = limit
```

Misfire listener:
```python
def on_misfire(event):
    metric_counter("flowbyte_scheduler_misfires_total", job_id=event.job_id).inc()
    log.error("job_misfire", job_id=event.job_id)
    # alert nếu > N/hour

scheduler.add_listener(on_misfire, EVENT_JOB_MISSED)
```

---

### 17.8 Validation rules v2 + `sync_runs` table

**Vấn đề (override §8.6):**
- `row_count_delta_check` luôn ~0% cho incremental (upsert không đổi count) → always-pass → **broken**.
- `required_fields_not_null_check` chạy POST-upsert → ngựa đã chạy.
- Không có baseline để so sánh volume bất thường.

**Phương án chốt — 4 rules mới:**

```python
VALIDATION_RULES = [
    # 1. Fetch → upsert parity: records extract phải bằng records upsert (±0)
    FetchUpsertParity(tolerance=0),

    # 2. Volume sanity: batch size không được >3× hoặc <0.2× median 7 ngày qua
    VolumeSanity(history_days=7, lower=0.2, upper=3.0, min_sample=3),

    # 3. Weekly full drift: full refresh count lệch >5% so với incremental running count
    WeeklyFullDrift(threshold_pct=5.0),

    # 4. Soft delete sanity: weekly full không được soft-delete >5% records
    SoftDeleteSanity(threshold_pct=5.0),
]
```

Pre-upsert reject `id=None` trong transform:
```python
def transform(self, raw: dict) -> dict | None:
    if not raw.get("id"):
        log.warning("transform_skip_no_id", raw_keys=list(raw.keys()))
        metric_counter("flowbyte_transform_skip_total", reason="no_id").inc()
        return None  # reject before upsert
    ...
```

`sync_runs` baseline table:
```sql
CREATE TABLE sync_runs (
    id bigserial PRIMARY KEY,
    pipeline text, resource text,
    mode text,                            -- incremental/full_refresh
    started_at timestamptz,
    finished_at timestamptz,
    records_fetched integer,
    records_upserted integer,
    records_soft_deleted integer,
    validation_status text,               -- pass/fail/skip
    validation_details jsonb,
    error text
);
CREATE INDEX ix_sync_runs_pipeline_resource_started ON sync_runs(pipeline, resource, started_at DESC);
```

---

### 17.9 Migration safety + entrypoint alert + healthcheck

**Vấn đề:** `alembic upgrade head` chạy trong entrypoint fail → container crashloop → không ai biết.

**Phương án chốt:**

`entrypoint.sh`:
```bash
#!/bin/bash
set -e

send_alert() {
    curl -sS -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
      -d "chat_id=${TELEGRAM_CHAT_ID}" \
      -d "text=🚨 Flowbyte deployment FAILED: $1" || true
}

trap 'send_alert "entrypoint error line $LINENO"' ERR

echo "Running alembic upgrade head..."
if ! alembic upgrade head; then
    send_alert "alembic upgrade head failed — container will exit"
    exit 1
fi

echo "Logging deployment_event..."
flowbyte ops log-deployment --version="$FLOWBYTE_VERSION" || true

exec flowbyte start
```

Schema:
```sql
CREATE TABLE deployment_events (
    id bigserial PRIMARY KEY,
    ts timestamptz DEFAULT now(),
    version text, git_sha text,
    status text,                          -- started/migrated/ready/failed
    error text
);
```

Healthcheck:
```yaml
healthcheck:
  test: ["CMD", "flowbyte", "ops", "health"]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 180s     # tăng từ 30s để cover alembic + cold start prime
```

---

### 17.10 Built-in backup + DR + master key escrow

**Vấn đề (override §10.5):** User phải tự viết backup script = 7 lỗi tiềm ẩn (no compression, no encryption, no retention, no verify, no off-site, no atomic write, no test). Master key mất = credentials mất vĩnh viễn.

**Phương án chốt:**

CLI commands:
```
flowbyte backup create    [--target=local|s3|both]
flowbyte backup list
flowbyte backup restore   <backup-id>
flowbyte backup verify    <backup-id>     # pg_restore --list + checksum
flowbyte backup prune     --policy=gfs
```

GFS retention:
```yaml
backup:
  schedule: "0 4 * * *"             # 4 AM giờ VN
  retention:
    daily: 7                        # 7 ngày gần nhất
    weekly: 4                       # 4 chủ nhật
    monthly: 6                      # 6 đầu tháng
  target:
    - type: local
      path: /data/backups
    - type: s3
      bucket: flowbyte-backups
      encryption: AES-256-GCM
  verify: true                      # pg_restore --list + row_count compare
```

Master key escrow:
```sql
CREATE TABLE master_key_metadata (
    key_id uuid PRIMARY KEY,
    created_at timestamptz,
    rotated_at timestamptz,
    status text,                          -- active/rotating/retired
    sha256_fingerprint text               -- verify key is correct
);
```

- `flowbyte creds rotate-key` — rotate master key, re-encrypt credentials
- Escrow procedure: encrypt master key với passphrase, split Shamir 3-of-5, gửi sysadmin

Monthly DR drill:
```yaml
cron:
  dr_drill: "0 5 1 * *"           # 5 AM ngày 1 hàng tháng
  action: "restore latest backup to ephemeral DB → verify row counts → report"
```

Schema:
```sql
CREATE TABLE backup_events (
    id bigserial PRIMARY KEY,
    ts timestamptz DEFAULT now(),
    action text,                          -- create/verify/prune/restore
    backup_id text, size_bytes bigint,
    checksum text, target text,
    status text, duration_ms integer,
    error text
);
```

---

### 17.11 Prometheus metrics + multi-signal health + `flowbyte inspect`

**Vấn đề:** SLO defined (§11) nhưng không có runtime metrics → không biết đang pass hay fail.

**Phương án chốt:**

Metrics endpoint:
```python
# flowbyte/metrics.py
from prometheus_client import Counter, Histogram, Gauge, start_http_server

sync_duration = Histogram("flowbyte_sync_duration_seconds", ["pipeline", "resource", "mode"])
sync_records  = Counter("flowbyte_sync_records_total", ["pipeline", "resource", "op"])
sync_errors   = Counter("flowbyte_sync_errors_total", ["pipeline", "resource", "kind"])
bucket_util   = Gauge("flowbyte_haravan_bucket_util", ["shop"])
heartbeat     = Gauge("flowbyte_daemon_heartbeat_timestamp")
reconciler    = Gauge("flowbyte_reconciler_tick_timestamp")
log_drops     = Counter("flowbyte_log_drops_total")

start_http_server(9090)
```

Multi-signal health:
```python
def health_check() -> HealthStatus:
    now = datetime.now(tz=tz)
    checks = {
        "daemon_process":   is_pid_alive(read_pid_file()),
        "heartbeat_fresh":  (now - last_heartbeat()) < timedelta(seconds=90),
        "reconciler_tick":  (now - last_reconciler_tick()) < timedelta(seconds=30),
        "no_stuck_claims":  count_claimed_requests(age_sec=600) == 0,
        "db_reachable":     can_select_1(),
    }
    return HealthStatus.from_checks(checks)
```

`flowbyte inspect`:
```
flowbyte inspect pipeline shop_main          # health + last sync + lag
flowbyte inspect resource shop_main:orders   # checkpoint + recent runs + errors
flowbyte inspect reconciler                  # queue depth + claimed ages
flowbyte inspect rate-limit                  # bucket state per shop
```

Alert enrichment (Telegram message include):
```
🚨 Flowbyte sync failed
Pipeline: shop_main / orders
Error: ConnectionTimeout (attempt 3/3)
Last success: 2026-04-24 09:15:12 +07
Checkpoint: 2026-04-24 08:58:00 (lag 22m)
Recovery count: 0
Bucket util: 87%
Runbook: https://wiki.../runbook#orders-timeout
```

Materialized view (ops dashboard):
```sql
CREATE MATERIALIZED VIEW mv_pipeline_health AS
SELECT pipeline, resource,
  MAX(finished_at) AS last_success_at,
  EXTRACT(EPOCH FROM (now() - MAX(finished_at))) AS lag_seconds,
  COUNT(*) FILTER (WHERE error IS NOT NULL AND started_at > now() - interval '1 hour') AS errors_1h
FROM sync_runs
WHERE validation_status = 'pass'
GROUP BY pipeline, resource;

-- Refresh mỗi 1 phút qua pg_cron hoặc scheduler job
```

---

### 17.12 Risk register v2 (additions)

Bổ sung vào §15:

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R7 | Bad migration cause crashloop | M | H | §17.9 entrypoint alert + `deployment_events` + manual rollback runbook |
| R8 | DB log sink backpressure blocks sync | M | M | §17.6 async bounded queue + drop metric + file sink fallback |
| R9 | Schedule collision at cold start overloads Haravan | H | M | §17.7 stagger + prime + misfire metric + APScheduler misfire_grace_time |
| R10 | Backup untested → silent data loss | H | Critical | §17.10 monthly DR drill + verify-backup + GFS + S3 off-site |
| R11 | Silent data skip from validation semantics | M | H | §17.8 4 new rules v2 + sync_runs baseline + pre-upsert reject |
| R12 | n8n co-tenant CPU contention | L | M | Monitor CPU/mem via Prometheus; document `docker-compose deploy.resources.limits` |

---

### 17.13 Implementation plan delta

Plan ban đầu (§16): **7 tuần**. V1.1 thêm +7-10 ngày effort → **8 tuần thực tế**.

| Phase | +Effort | Mục delta |
|---|---|---|
| 1a Foundation | +2d | §17.2 non-root Docker + §17.3 timezone config + §17.9 entrypoint+alert |
| 1b Haravan client | +1d | §17.5 keyset + §17.7 prime + robust regex |
| 2a Sync engine | +2d | §17.1 `_sync_id` marker + §17.4 recovery + §17.5 composite cursor + §17.6 async log sink |
| 2b 5 resources + Weekly Full | +1d | §17.8 4 validation rules v2 + `sync_runs` table |
| 3 Scheduler + Alerting | +1d | §17.7 stagger/misfire + §17.11 metrics + inspect CLI + alert enrichment |
| 4 Packaging + Deployment | +2-3d | §17.10 backup CLI + GFS + DR drill + master key escrow |

Go-live gate:
- [ ] DR drill thành công ít nhất 1 lần
- [ ] 48h soak test, error rate < 1%, zero stuck claims
- [ ] Prometheus dashboard xanh cho tất cả SLOs
- [ ] Validation rules v2 pass 7 ngày liên tục
- [ ] Runbook cho 12 risk scenarios (R1-R12) đã viết + review

---

## Changelog

| Ngày | Version | Thay đổi |
|---|---|---|
| 2026-04-24 | v1 | TDD đầu tiên; 16 sections theo khung OPA + 8 kiến trúc đã chốt + dòng "💡 Tại sao" mỗi section lớn |
| 2026-04-24 | **1.2** | **E2E verified với Haravan API thật:** (1) Auth header đổi toàn bộ từ `X-Haravan-Access-Token` → `Authorization: Bearer` (§7.1, §7.3, cold start prime) — Haravan Omnichannel thực tế dùng Bearer auth, không phải header trong docs cũ; (2) `orders.order_number` Haravan trả `"#85983"` (string có prefix `#`) → kiểu phải là `VARCHAR/TEXT`, không phải `INTEGER`. Cả hai lỗi chỉ phát hiện qua E2E, unit test không bắt được. |
| 2026-04-24 | 1.1 | Senior review — 11 amendments §17: full-refresh `_sync_id` marker, bootstrap non-root, timezone config, sync_request recovery, keyset pagination, async log sink, schedule stagger, validation rules v2, migration safety, built-in backup + DR, Prometheus observability |
