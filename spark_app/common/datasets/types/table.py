from __future__ import annotations

from typing import TYPE_CHECKING, Any

from spark_app.common.datasets.models import Dataset, ResolveContext, apply_templates

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_RESERVED_KEYS = {"type", "table", "path"}


class TableDataset(Dataset):
    type = "table"
    default_format = "iceberg"

    def __init__(
        self,
        name: str,
        location: str,
        metadata: dict[str, Any] | None = None,
        by_path: bool = False,
    ) -> None:
        super().__init__(name, location, metadata)
        # `by_path` distinguishes a catalog table name (spark.table / writeTo) from a
        # storage location that must be read/written via a format loader (base IO).
        self.by_path = by_path

    @classmethod
    def from_spec(cls, name: str, spec: dict, context: ResolveContext) -> "TableDataset":
        table = spec.get("table")
        path = spec.get("path")

        if not table and not path:
            raise ValueError(f"dataset '{name}' table spec requires 'table' or 'path'")

        if table:
            table_name = str(table)
            catalog = context.catalog
            if catalog and not table_name.startswith(f"{catalog}."):
                location = f"{catalog}.{table_name}"
            else:
                location = table_name
            by_path = False
        else:
            relative_path = apply_templates(str(path), context).lstrip("/")
            location = f"{context.warehouse}/{relative_path}"
            by_path = True

        metadata = {key: value for key, value in spec.items() if key not in _RESERVED_KEYS}
        catalog = context.catalog
        if catalog:
            metadata.setdefault("catalog", catalog)

        return cls(name=name, location=location, metadata=metadata, by_path=by_path)

    def read(self, spark: SparkSession) -> DataFrame:
        if self.by_path:
            return super().read(spark)  # generic format loader
        return spark.table(self.location)

    def write(self, spark: SparkSession, df: DataFrame) -> None:
        if self.by_path:
            super().write(spark, df)  # generic format writer
            return

        writer = df.writeTo(self.location)
        mode = self.metadata.get("mode", self.default_mode)
        if mode == "append":
            writer.append()
        elif mode == "overwrite_partitions":
            writer.overwritePartitions()
        else:
            raise ValueError(
                f"dataset '{self.name}' unsupported table write mode {mode!r} (supported: append, overwrite_partitions)"
            )
