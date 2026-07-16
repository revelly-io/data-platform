"""Apply or drop Iceberg DDL files under catalog/ddl/."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from string import Template

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from spark_app.common.config.loader import ConfigLoader, _expand_env
from spark_app.common.config.merge import load_yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

DDL_ROOT = ConfigLoader.REPO_ROOT / "catalog" / "ddl"
LAYERS = ("raw", "refined", "mart")


def _load_global_config(env: str) -> dict:
    env_file = ConfigLoader.REPO_ROOT / f".env.{env}"
    if not env_file.is_file():
        raise FileNotFoundError(f"Missing {env_file} (copy .env.{env}.example → .env.{env})")
    load_dotenv(env_file)

    global_config_path = ConfigLoader.GLOBAL_CONFIG_ROOT / env / ConfigLoader.GLOBAL_CONFIG_FILE
    if not global_config_path.is_file():
        raise FileNotFoundError(f"Missing required file: {global_config_path}")

    return _expand_env(load_yaml(global_config_path))


def _build_spark(config: dict) -> SparkSession:
    spark_config = config.get("spark", {})
    master = spark_config.get("master", "local[*]")
    configs = spark_config.get("configs") or {}

    builder = SparkSession.builder.appName("catalog.apply").master(master)
    for key, value in configs.items():
        builder = builder.config(key, str(value))
    return builder.getOrCreate()


def _resolve_sql_files(layer: str | None, domain: str | None, table: str | None) -> list[Path]:
    if layer and layer not in LAYERS:
        raise ValueError(f"layer must be one of {LAYERS}, got {layer!r}")
    if table and not domain:
        raise ValueError("--table requires --domain")
    if domain and not layer:
        raise ValueError("--domain requires --layer")

    if layer and domain and table:
        path = DDL_ROOT / layer / domain / f"{table}.sql"
        if not path.is_file():
            raise FileNotFoundError(f"DDL not found: {path}")
        return [path]

    if layer and domain:
        directory = DDL_ROOT / layer / domain
        if not directory.is_dir():
            raise FileNotFoundError(f"DDL directory not found: {directory}")
        return sorted(directory.glob("*.sql"))

    if layer:
        return sorted((DDL_ROOT / layer).rglob("*.sql"))

    return sorted(DDL_ROOT.rglob("*.sql"))


def _substitute_sql(sql: str, config: dict, layer: str, domain: str, table: str) -> str:
    warehouse = config.get("datasets", {}).get("warehouse", "")
    values = {
        "warehouse": str(warehouse).rstrip("/"),
        "layer": layer,
        "domain": domain,
        "table": table,
    }
    return Template(sql).safe_substitute(values)


def _ensure_namespaces(spark: SparkSession, catalog: str, layer: str, domain: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{layer}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{layer}.{domain}")


def _apply_file(
    spark: SparkSession,
    path: Path,
    config: dict,
    catalog: str,
) -> None:
    layer = path.parts[-3]
    domain = path.parts[-2]
    table = path.stem
    _ensure_namespaces(spark, catalog, layer, domain)
    sql = _substitute_sql(path.read_text(encoding="utf-8"), config, layer, domain, table)
    logger.info("Applying %s", path.relative_to(DDL_ROOT))
    for statement in _split_statements(sql):
        spark.sql(statement)


def _drop_table(spark: SparkSession, catalog: str, layer: str, domain: str, table: str) -> None:
    qualified = f"{catalog}.{layer}.{domain}.{table}"
    logger.info("Dropping %s", qualified)
    spark.sql(f"DROP TABLE IF EXISTS {qualified}")


def _split_statements(sql: str) -> list[str]:
    parts = [part.strip() for part in sql.split(";")]
    return [part for part in parts if part]


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply or drop Iceberg DDL under catalog/ddl/")
    parser.add_argument("--env", required=True)
    parser.add_argument("--layer", choices=LAYERS)
    parser.add_argument("--domain")
    parser.add_argument("--table")
    parser.add_argument("--drop", action="store_true", help="Drop table(s) instead of applying DDL")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    config = _load_global_config(args.env)
    catalog = (config.get("catalog") or {}).get("name", "iceberg")

    spark = _build_spark(config)
    try:
        if args.drop:
            if not (args.layer and args.domain and args.table):
                raise ValueError("--drop requires --layer, --domain, and --table")
            _drop_table(spark, catalog, args.layer, args.domain, args.table)
            return

        sql_files = _resolve_sql_files(args.layer, args.domain, args.table)
        if not sql_files:
            raise FileNotFoundError(f"No DDL files found under {DDL_ROOT}")

        for path in sql_files:
            _apply_file(spark, path, config, catalog)
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.error("%s", exc)
        sys.exit(1)
