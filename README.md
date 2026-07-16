# data-platform

A data platform repository for running Spark apps.

Framework design (config merge, datasets, components): [docs/spark-app-framework.md](docs/spark-app-framework.md).

## Prerequisites

Install these on the host before `mise run setup`. System Python is not required.

| Tool | Purpose | Install (macOS) |
| ---- | ------- | --------------- |
| [mise](https://mise.jdx.dev/) | Python version + task runner | `brew install mise` |
| [uv](https://docs.astral.sh/uv/) | venv + package sync | `brew install uv` |
| OpenJDK | PySpark (JVM) — not needed for Jupyter/DuckDB only | `brew install openjdk` |

Configure mise in `~/.zshrc`:

```bash
eval "$(mise activate zsh)"
```

Then `source ~/.zshrc`.

## Host setup

One-time project setup:

```bash
cd data-platform
mise trust          # trust mise.toml
mise run setup      # Python 3.12 + .venv + packages
```

`mise run setup` runs:

1. `mise install` — Python 3.12 (from `mise.toml`)
2. `uv sync` — create `.venv` and install packages from `pyproject.toml`

Packages installed into `.venv` include:

| Group | Packages |
| ----- | -------- |
| runtime | `pyspark`, `pyyaml` |
| dev | `pytest`, `ruff`, `duckdb`, `jupyterlab` |

`mise run setup` does **not** install or start: OpenJDK, Docker, or Jupyter. Use `mise run jupyter` when you want a notebook server.

Entering the project directory auto-activates `.venv` (`python.uv_venv_auto`).

### Jupyter (after setup)

```bash
mise run jupyter   # opens notebooks/ — use `import duckdb` in a notebook
```

## Docker Compose

Local Iceberg infra (MinIO + Postgres + REST catalog) for `--env local` integration tests — **coming soon**.

```bash
# docker compose up -d
```

Until then, `mise run sample` runs Spark in `local[*]` mode without remote storage.

## Run apps

Requires OpenJDK. Use `--env local` on the Mac; `--env sandbox` targets homelab (prod-like K8s Spark).

### Sample app

```bash
mise run sample
```

Runs `sample.hello_world` with today's `--ymd` and `--hms`.

### Any app

```bash
mise run spark-app \
  --app_name sample.hello_world \
  --env local \
  --ymd 2026-07-14 \
  --hms 100000
```

Extra CLI flags (key-value pairs after required args):

```bash
mise run spark-app \
  --app_name sample.hello_world \
  --env local \
  --ymd 2026-07-14 \
  --hms 100000 \
  --bucket my-bucket
```

### CLI arguments

| Argument | Required | Description |
| -------- | -------- | ----------- |
| `--app_name` | yes | App package path (e.g. `sample.hello_world`) |
| `--env` | yes | `local` (Mac) or `sandbox` (homelab) |
| `--ymd` | yes | Date (`YYYY-MM-DD`) |
| `--hms` | yes | Time (`HHMMSS`) |
| `--key value` | no | Extra args passed to `self._extra_args` |

Config merge, app layout, `self.input` / `self.output`, and startup logs: [docs/spark-app-framework.md](docs/spark-app-framework.md).

## mise tasks

| Task | Description |
| ---- | ----------- |
| `mise run setup` | Install Python 3.12, sync `.venv` (pyspark, duckdb, jupyterlab, …) |
| `mise run jupyter` | Start JupyterLab (`notebooks/`) |
| `mise run lint` | Ruff check --fix and format |
| `mise run test` | Run pytest |
| `mise run sample` | Run `sample.hello_world` (`--env local`) |
| `mise run spark-app -- ...` | Run any Spark app |
| `mise run clean-cache` | Remove pytest/ruff/`__pycache__` caches |
