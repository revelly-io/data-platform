from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class ResolveContext:
    def __init__(self, config: dict, env: str, ymd: str, hms: str) -> None:
        self.config = config
        self.env = env
        self.ymd = ymd
        self.hms = hms

    @property
    def warehouse(self) -> str:
        warehouse = self.config.get("datasets", {}).get("warehouse")
        if not warehouse:
            raise ValueError("datasets.warehouse is required to resolve path datasets")
        return str(warehouse).rstrip("/")

    @property
    def catalog(self) -> str | None:
        catalog = self.config.get("catalog") or {}
        return catalog.get("name") or catalog.get("default")


def apply_templates(value: str, context: ResolveContext) -> str:
    return value.format(ymd=context.ymd, hms=context.hms, env=context.env)


class Dataset(ABC):
    """A resolved dataset that also knows how to read/write itself.

    The base implements the generic batch IO path — ``read.format(...).options(...).load()``
    and ``write.format(...).mode(...).options(...).partitionBy(...).save()`` — driven entirely
    by the yaml spec, so most types need no read/write code at all. Types whose IO differs
    (e.g. kafka streaming, catalog tables) override read/write.

    Subclasses live in one module per `type` (spark_app/common/datasets/types/<type>.py) and
    build themselves via `from_spec`. Resolution (config -> location/metadata) happens there;
    `read`/`write` bind that to a SparkSession at run time.
    """

    type: str = "dataset"
    default_format: str | None = None
    default_mode: str = "overwrite"

    def __init__(self, name: str, location: str, metadata: dict[str, Any] | None = None) -> None:
        self.name = name
        self.location = location
        self.metadata = metadata or {}

    def read(self, spark: SparkSession) -> DataFrame:
        reader = spark.read
        fmt = self.metadata.get("format", self.default_format)
        if fmt:
            reader = reader.format(fmt)
        reader = reader.options(**self._options())
        return reader.load(self.location)

    def write(self, spark: SparkSession, df: DataFrame) -> None:
        writer = df.write.mode(self.metadata.get("mode", self.default_mode))
        fmt = self.metadata.get("format", self.default_format)
        if fmt:
            writer = writer.format(fmt)
        writer = writer.options(**self._options())
        partition_by = self.metadata.get("partition_by")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(self.location)

    def _options(self) -> dict[str, str]:
        """Arbitrary reader/writer options passed straight through from the spec."""
        return {str(key): str(value) for key, value in (self.metadata.get("options") or {}).items()}

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r}, location={self.location!r})"
