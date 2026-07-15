"""Helpers for interactive (Jupyter) exploration.

from pyspark.sql import SparkSession
from spark_app.common.notebook import build_dataset_context

spark = SparkSession.builder.master("local[*]").getOrCreate()
ds = build_dataset_context(
    "sample.hello_world", env="local", ymd="2026-07-15", hms="000000", spark=spark
)

ds.input.read("orders").show()
ds("sandbox").input.read("orders").show()
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from spark_app.common.config.loader import ConfigLoader
from spark_app.common.datasets import DatasetContext

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

__all__ = ["build_dataset_context"]


def build_dataset_context(
    app_name: str,
    env: str,
    ymd: str,
    hms: str,
    spark: SparkSession,
) -> DatasetContext:
    """Build the same DatasetContext an app uses at run time."""
    config = ConfigLoader.load(app_name, env)
    return DatasetContext(app_name=app_name, env=env, ymd=ymd, hms=hms, spark=spark, config=config)
