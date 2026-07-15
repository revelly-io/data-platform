# data-platform

A data platform repository for running Spark apps.

## Prerequisites (macOS)

- [mise](https://mise.jdx.dev/) — Python version management and task runner
- [uv](https://docs.astral.sh/uv/) — Python package and venv management
- Java — required for PySpark (`brew install openjdk`)

## 1. Install mise and configure your shell

```bash
brew install mise
```

Add the following to `~/.zshrc`:

```bash
eval "$(mise activate zsh)"
```

Apply the change:

```bash
source ~/.zshrc
```

## 2. Install uv

```bash
brew install uv
```

Or:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 3. Set up the project

```bash
cd data-platform
mise trust          # one-time: trust mise.toml
mise run setup      # install Python 3.12 + uv sync
```

`setup` runs:

- `mise install` — installs Python defined in `mise.toml`
- `uv sync` — creates `.venv` and installs dependencies (including dev tools)

When you enter the project directory, mise auto-activates `.venv` (`python.uv_venv_auto`).

## 4. Run a Spark app

### Sample app (hello_world)

```bash
mise run sample
```

Runs `sample.hello_world` locally. `--ymd` and `--hms` are set to the current date and time.

### Run manually

```bash
mise run spark-app \
  --app_name sample.hello_world \
  --env local \
  --ymd 2026-07-14 \
  --hms 100000
```

You can also pass extra args:

```bash
mise run spark-app \
  --app_name sample.hello_world \
  --env local \
  --ymd 2026-07-14 \
  --hms 100000 \
  --table users
```

## CLI arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--app_name` | yes | App package path (e.g. `sample.hello_world`) |
| `--env` | yes | `local` or `sandbox` |
| `--ymd` | yes | Date (`YYYY-MM-DD`) |
| `--hms` | yes | Time (`HHMMSS`) |
| `--key value` | no | Extra args (key-value pairs) |

## App structure

Each Spark app follows this layout:

```
spark_app/
└── sample/hello_world/
    ├── app.py        # exactly one SparkAppBase subclass
    └── config.yaml   # static config (input/output, spark settings, etc.)
```

Run any app:

```bash
mise run spark-app --app_name <package.path> --env <env> --ymd <YYYY-MM-DD> --hms <HHMMSS>
```

## mise tasks

| Task | Description |
|------|-------------|
| `mise run setup` | Install Python and dependencies |
| `mise run lint` | Run ruff check --fix and format |
| `mise run sample` | Run `sample.hello_world` locally |
| `mise run spark-app -- ...` | Run any Spark app |
