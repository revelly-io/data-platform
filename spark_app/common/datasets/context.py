from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from spark_app.common.config.loader import ConfigLoader
from spark_app.common.datasets.models import Dataset, ResolveContext
from spark_app.common.datasets.registry import detect_type, get_dataset_class

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

DatasetKind = Literal["input", "output"]


class _DatasetBinding:
    """Shared resolve/cache state for input and output DatasetContext instances."""

    def __init__(
        self,
        app_name: str,
        env: str,
        ymd: str,
        hms: str,
        spark: SparkSession,
        config: dict | None = None,
    ) -> None:
        self.app_name = app_name
        self.env = env
        self.ymd = ymd
        self.hms = hms
        self.spark = spark
        self._config = config
        self._cache: dict[str, dict[str, dict[str, Dataset]]] = {}

    def resolve(self, kind: DatasetKind, env: str | None = None) -> dict[str, Dataset]:
        target_env = env or self.env
        env_cache = self._cache.setdefault(target_env, {})
        if kind not in env_cache:
            config = self._config_for_env(target_env)
            env_cache[kind] = self._resolve_group(kind, config, target_env)
        return env_cache[kind]

    def _config_for_env(self, env: str) -> dict:
        if env == self.env and self._config is not None:
            return self._config
        if not self.app_name:
            raise RuntimeError("cross-env access requires app_name")
        return ConfigLoader.load(self.app_name, env)

    def _resolve_group(self, kind: DatasetKind, config: dict, env: str) -> dict[str, Dataset]:
        resolve_ctx = ResolveContext(config=config, env=env, ymd=self.ymd, hms=self.hms)
        specs = (config.get("datasets") or {}).get(kind) or {}
        resolved: dict[str, Dataset] = {}
        for name, spec in specs.items():
            if not isinstance(spec, dict):
                raise ValueError(f"dataset '{name}' spec must be a mapping, got {type(spec).__name__}")
            dataset_type = detect_type(spec)
            dataset_cls = get_dataset_class(dataset_type)
            resolved[name] = dataset_cls.from_spec(name, spec, resolve_ctx)
        return resolved


class DatasetContext:
    """Resolved datasets for one yaml group (``input`` or ``output``).

    Default run env:
        ctx.read("orders")
        ctx.write("main", df)

    Another env:
        ctx("homelab").read("orders")
    """

    INPUT: DatasetKind = "input"
    OUTPUT: DatasetKind = "output"

    def __init__(
        self,
        binding: _DatasetBinding,
        kind: DatasetKind,
        env: str | None = None,
    ) -> None:
        self._binding = binding
        self._kind = kind
        self._env = env
        self._datasets()  # resolve eagerly so bad config fails at build time

    @classmethod
    def pair(
        cls,
        app_name: str,
        env: str,
        ymd: str,
        hms: str,
        spark: SparkSession,
        config: dict | None = None,
    ) -> tuple[DatasetContext, DatasetContext]:
        binding = _DatasetBinding(app_name, env, ymd, hms, spark, config)
        return cls(binding, cls.INPUT), cls(binding, cls.OUTPUT)

    @classmethod
    def from_merged_config(
        cls,
        config: dict,
        kind: DatasetKind,
        env: str,
        ymd: str,
        hms: str,
        spark: SparkSession | None = None,
    ) -> DatasetContext:
        """Build one side from an already-merged config (tests, notebooks)."""
        if spark is None:
            raise ValueError("spark is required")
        binding = _DatasetBinding("", env, ymd, hms, spark, config)
        return cls(binding, kind)

    def __call__(self, env: str) -> DatasetContext:
        return DatasetContext(self._binding, self._kind, env=env)

    def __getitem__(self, name: str) -> Dataset:
        return self._datasets()[name]

    @property
    def resolved(self) -> dict[str, Dataset]:
        return self._datasets()

    def read(self, name: str) -> DataFrame:
        if self._kind != self.INPUT:
            raise TypeError("read() is only available on input DatasetContext")
        try:
            return self._datasets()[name].read(self._binding.spark)
        except KeyError:
            raise KeyError(f"input dataset '{name}' not found (available: {sorted(self._datasets())})") from None

    def write(self, name: str, df: DataFrame) -> None:
        if self._kind != self.OUTPUT:
            raise TypeError("write() is only available on output DatasetContext")
        try:
            self._datasets()[name].write(self._binding.spark, df)
        except KeyError:
            raise KeyError(f"output dataset '{name}' not found (available: {sorted(self._datasets())})") from None

    def _datasets(self) -> dict[str, Dataset]:
        return self._binding.resolve(self._kind, self._env)
