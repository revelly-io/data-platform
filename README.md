# data-platform

A data platform repository for running Spark apps.

For framework design (components, datasets, config merge), see [docs/spark-app-framework.md](docs/spark-app-framework.md).

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
  --bucket my-bucket
```

## CLI arguments

| Argument | Required | Description |
| -------- | -------- | ----------- |
| `--app_name` | yes | App package path (e.g. `sample.hello_world`) |
| `--env` | yes | `local` or `sandbox` |
| `--ymd` | yes | Date (`YYYY-MM-DD`) |
| `--hms` | yes | Time (`HHMMSS`) |
| `--key value` | no | Extra args (key-value pairs) |

### Using extra args in `app.py`

CLI flags after the required args are parsed into `self._extra_args` as a `dict[str, str]`:

```python
class MyApp(SparkAppBase):
    def run(self, spark: SparkSession) -> None:
        bucket = self._extra_args["bucket"]
        mode = self._extra_args.get("mode", "incremental")
```

`self._config` is the merged global + app config. `self._extra_args` holds per-run CLI overrides.

## Config

Config is merged in two layers by `--env`:

```
spark_app/config/{local|sandbox}/global-config.yaml
  + spark_app/{app_name}/config.yaml
  → deep merge (app overrides global)
```

| Section | Typical location | Purpose |
| ------- | ---------------- | ------- |
| `catalog` | global | Catalog name (used when resolving table datasets) |
| `datasets.warehouse` | global | Base path for file-based datasets |
| `datasets.input` / `datasets.output` | app | Named input/output dataset specs |
| `spark` | global + app | `master`, `configs` for SparkSession |

Example app `config.yaml`:

```yaml
spark:
  configs:
    spark.sql.shuffle.partitions: "1"

datasets:
  input:
    orders:
      type: table
      table: raw.orders
  output:
    main:
      type: path
      path: mart/hello_world/ymd={ymd}
      format: parquet
      mode: overwrite
```

At startup, merged config and resolved dataset locations are logged (see [docs/spark-app-framework.md](docs/spark-app-framework.md#startup-log-merged--resolved)).

## App layout

Each Spark app is a package under `spark_app/`:

```
spark_app/
└── sample/hello_world/
    ├── app.py        # exactly one SparkAppBase subclass
    └── config.yaml
```

Package depth is not limited (`mart.orders.daily_summary` → nested directories). Intermediate dirs need `__init__.py`.

## Datasets in app code

Subclass `SparkAppBase` and implement `run()`. Use `self.input` / `self.output`:

```python
class HelloWorldApp(SparkAppBase):
    def run(self, spark: SparkSession) -> None:
        orders = self.input.read("orders")
        self.output.write("main", orders)

        # optional: read from another env
        sandbox_orders = self.input("sandbox").read("orders")
```

Each of `self.input` and `self.output` is a `DatasetContext` built from `datasets.input` / `datasets.output` in config. IO is implemented on `Dataset`; the context resolves yaml specs to `Dataset` objects at execute time.

## mise tasks

| Task | Description |
| ---- | ----------- |
| `mise run setup` | Install Python and dependencies |
| `mise run lint` | Run ruff check --fix and format |
| `mise run test` | Run pytest |
| `mise run sample` | Run `sample.hello_world` locally |
| `mise run spark-app -- ...` | Run any Spark app |
