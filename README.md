# Flowbyte

ETL tool syncing Haravan → PostgreSQL. CLI-first, self-hosted.

---

## Deployment

### Prerequisites

- Ubuntu 22.04 LTS
- Docker Engine 24+
- docker compose plugin (v2)
- User with UID 1000 and Docker group membership

### VPS directory layout

```
/opt/flowbyte/
├── docker-compose.yml      # copy from repo
├── .env                    # secrets — see below
├── config/
│   ├── config.yml          # global config
│   └── pipelines/          # one YAML per pipeline
└── /etc/flowbyte/
    └── master.key          # chmod 600, owned by UID 1000
```

### Required `.env` variables

```dotenv
POSTGRES_PASSWORD=<strong-password>
MASTER_KEY_PATH=/etc/flowbyte/master.key   # host path, mounted :ro

# Optional — enables Telegram alert on deployment failure
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# Set to the image tag being deployed
FLOWBYTE_VERSION=1.1.0
```

### First-time setup

```bash
# 1. Pull or build the image
docker build --build-arg FLOWBYTE_VERSION=1.1.0 -t flowbyte:1.1.0 .

# 2. Copy compose file to VPS
scp docker-compose.yml user@vps:/opt/flowbyte/

# 3. On VPS — create master key
sudo mkdir -p /etc/flowbyte
sudo openssl rand -base64 32 > /etc/flowbyte/master.key
sudo chmod 600 /etc/flowbyte/master.key
sudo chown 1000:1000 /etc/flowbyte/master.key

# 4. Start services (runs alembic upgrade head automatically)
cd /opt/flowbyte
docker compose up -d
```

### Install as systemd service

```bash
# Copy unit file
sudo cp docker/flowbyte.service /etc/systemd/system/flowbyte.service

# Reload and enable
sudo systemctl daemon-reload
sudo systemctl enable --now flowbyte

# Check status
sudo systemctl status flowbyte
```

### Verify health

```bash
docker compose exec flowbyte flowbyte health --strict
```

Exit code 0 = healthy. Exit code 3 = unhealthy (DB unreachable or scheduler dead).

### Admin operations (credentials, pipeline init)

The `flowbyte-admin` service runs with the same image but with read-write config mount
and is excluded from default `up`:

```bash
docker compose --profile admin run --rm flowbyte-admin flowbyte creds set <ref>
```
