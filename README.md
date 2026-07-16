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

## Local MinIO stack

Sample parquet files live in `fixtures/` (committed). Docker Compose starts MinIO and seeds the `local` bucket (`s3a://local/raw/...`) on first boot.

Requires `.env.local` from [Host setup](#host-setup).

```bash
mise run infra:up
```

MinIO console: http://localhost:9001 (credentials in `.env.local`).

## Run apps

Requires OpenJDK. Use `--env local` on the Mac; `--env homelab` or `--env aws` for remote targets (see `spark_app/config/{env}/`).

Start MinIO before running the sample app (`mise run infra:up`).

### Sample app

```bash
mise run sample
```

Reads `fixtures/orders.parquet` from MinIO (`s3a://local/raw/fixtures/orders/...`), aggregates by order status, and prints the result.

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
| `mise run infra:up` | Start MinIO + copy fixtures into bucket |
| `mise run infra:down` | Stop MinIO stack |
| `mise run jupyter` | Start JupyterLab (`notebooks/`) |
| `mise run lint` | Ruff check --fix and format |
| `mise run test` | Run pytest |
| `mise run sample` | Run `sample.orders_summary` (`--env local`, needs MinIO) |
| `mise run spark-app -- ...` | Run any Spark app |
| `mise run clean-cache` | Remove pytest/ruff/`__pycache__` caches |
