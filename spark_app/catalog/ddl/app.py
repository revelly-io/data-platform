"""Apply or drop Iceberg DDL files under catalog/ddl/."""

from __future__ import annotations

import logging
from pathlib import Path
from string import Template

from pyspark.sql import SparkSession

from spark_app.common.bases.ops import SparkOpsAppBase
from spark_app.common.config.loader import ConfigLoader

logger = logging.getLogger(__name__)

DDL_ROOT = ConfigLoader.REPO_ROOT / "catalog" / "ddl"
LAYERS = ("raw", "refined", "mart")


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


def _is_truthy(value: str | None) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


class CatalogDdlApp(SparkOpsAppBase):
    """Run Iceberg table DDL under catalog/ddl/: apply (CREATE) or drop (--drop true).

    --layer/--domain/--table/--drop are passed through AppFactory's extra_args
    (e.g. `--app_name catalog.ddl --env local --layer refined --domain sample --table orders`).
    """

    def run(self, spark: SparkSession) -> None:
        catalog = (self.config.get("catalog") or {}).get("name", "iceberg")
        layer = self.extra_args.get("layer")
        domain = self.extra_args.get("domain")
        table = self.extra_args.get("table")
        drop = _is_truthy(self.extra_args.get("drop"))

        if drop:
            if not (layer and domain and table):
                raise ValueError("--drop requires --layer, --domain, and --table")
            _drop_table(spark, catalog, layer, domain, table)
            return

        sql_files = _resolve_sql_files(layer, domain, table)
        if not sql_files:
            raise FileNotFoundError(f"No DDL files found under {DDL_ROOT}")

        for path in sql_files:
            _apply_file(spark, path, self.config, catalog)
