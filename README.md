# data-platform

A data platform repository for running Spark apps.

Framework design (config merge, datasets, components): [docs/spark-app-framework.md](docs/spark-app-framework.md).

## Prerequisites

Install these on the host before `mise run setup`. System Python is not required.

| Tool | Purpose | Install (macOS) |
| ---- | ------- | --------------- |
| [mise](https://mise.jdx.dev/) | Python version + task runner | `brew install mise` |
| [uv](https://docs.astral.sh/uv/) | venv + package sync | `brew install uv` |
| OpenJDK | PySpark (JVM) | `brew install openjdk` |
| Docker | Local MinIO stack | [Docker Desktop](https://www.docker.com/products/docker-desktop/) |

Configure mise in `~/.zshrc`:

```bash
eval "$(mise activate zsh)"
```

Then `source ~/.zshrc`.

## Host setup

One-time project setup:

```bash
cd data-platform
mise trust                       # trust mise.toml
mise run setup                   # Python 3.12 + .venv + packages
cp .env.local.example .env.local # required — Spark + MinIO credentials
```

Each `--env` reads its own secrets file (gitignored): `.env.local`, `.env.homelab`, …  
Templates are committed as `.env.<env>.example`.

`mise run setup` runs:

1. `mise install` — Python 3.12 (from `mise.toml`)
2. `uv sync` — create `.venv` and install packages from `pyproject.toml`

Packages installed into `.venv` include:

| Group | Packages |
| ----- | -------- |
| runtime | `pyspark` (4.1.2), `pyyaml`, `python-dotenv` |
| dev | `pytest`, `ruff`, `jupyterlab` |

`mise run setup` does **not** install or start: OpenJDK, Docker, or Jupyter. Use `mise run jupyter` when you want a notebook server.

Entering the project directory auto-activates `.venv` (`python.uv_venv_auto`).

### Jupyter (after setup)

```bash
mise run jupyter   # opens notebooks/
```

## Local stack

Sample parquet lives in `fixtures/` (committed). Docker Compose starts **MinIO**, **Postgres**, and **Iceberg REST Catalog**, and seeds the `local` bucket on first boot.

Requires `.env.local` from [Host setup](#host-setup).

```bash
mise run infra:up
```

| Service | URL |
| ------- | --- |
| MinIO API | http://localhost:9000 |
| MinIO console | http://localhost:9001 |
| Iceberg REST | http://localhost:8181 |

Apply Iceberg DDL (once per fresh stack, or after DDL changes):

```bash
mise run catalog:apply --env local --layer refined --domain sample --table orders
```

## Run apps

Requires OpenJDK. Use `--env local` on the Mac; `--env homelab` or `--env aws` for remote targets (see `spark_app/config/{env}/`).

Start the local stack before running apps (`mise run infra:up`).

### Sample apps

**Ingest** — raw parquet → refined Iceberg table:

```bash
mise run catalog:apply --env local --layer refined --domain sample --table orders
mise run sample:ingest
```

**Summary** — read parquet from MinIO, aggregate, print:

```bash
mise run sample
```

Reads `s3a://local/raw/sample/orders/orders.parquet`. Ingest writes to `iceberg.refined.sample.orders`.

### Any app

```bash
mise run spark-app \
  --app_name sample.orders_summary \
  --env local \
  --ymd 2026-07-14 \
  --hms 100000
```

Extra CLI flags (key-value pairs after required args):

```bash
mise run spark-app \
  --app_name sample.orders_summary \
  --env local \
  --ymd 2026-07-14 \
  --hms 100000 \
  --bucket my-bucket
```

### CLI arguments

| Argument | Required | Description |
| -------- | -------- | ----------- |
| `--app_name` | yes | App package path (e.g. `sample.orders_summary`) |
| `--env` | yes | `local`, `homelab`, or `aws` |
| `--ymd` | yes | Date (`YYYY-MM-DD`) |
| `--hms` | yes | Time (`HHMMSS`) |
| `--key value` | no | Extra args passed to `self._extra_args` |

Config merge, app layout, `self.input` / `self.output`, and startup logs: [docs/spark-app-framework.md](docs/spark-app-framework.md).

## mise tasks

| Task | Description |
| ---- | ----------- |
| `mise run setup` | Install Python 3.12, sync `.venv` |
| `mise run infra:up` | Start MinIO + Postgres + Iceberg REST + seed fixtures |
| `mise run infra:down` | Stop local stack |
| `mise run catalog:apply --env local --layer … --domain … --table …` | Apply Iceberg DDL |
| `mise run catalog:drop --env local --layer … --domain … --table …` | Drop Iceberg table |
| `mise run sample:ingest` | Run `sample.orders_ingest` (needs catalog:apply) |
| `mise run sample` | Run `sample.orders_summary` |
| `mise run spark-app -- ...` | Run any Spark app |
| `mise run clean-cache` | Remove pytest/ruff/`__pycache__` caches |
