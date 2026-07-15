from __future__ import annotations

from typing import TYPE_CHECKING

from spark_app.common.config.loader import ConfigLoader
from spark_app.common.datasets.models import Dataset, ResolveContext
from spark_app.common.datasets.registry import detect_type, get_dataset_class

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class _InputSide:
    def __init__(self, datasets: dict[str, Dataset], spark: SparkSession) -> None:
        self._datasets = datasets
        self._spark = spark

    def __getitem__(self, name: str) -> Dataset:
        return self._datasets[name]

    @property
    def resolved(self) -> dict[str, Dataset]:
        return self._datasets

    def read(self, name: str) -> DataFrame:
        try:
            dataset = self._datasets[name]
        except KeyError:
            raise KeyError(f"input dataset '{name}' not found (available: {sorted(self._datasets)})") from None
        return dataset.read(self._spark)


class _OutputSide:
    def __init__(self, datasets: dict[str, Dataset], spark: SparkSession) -> None:
        self._datasets = datasets
        self._spark = spark

    def __getitem__(self, name: str) -> Dataset:
        return self._datasets[name]

    @property
    def resolved(self) -> dict[str, Dataset]:
        return self._datasets

    def write(self, name: str, df: DataFrame) -> None:
        try:
            dataset = self._datasets[name]
        except KeyError:
            raise KeyError(f"output dataset '{name}' not found (available: {sorted(self._datasets)})") from None
        dataset.write(self._spark, df)


class _EnvView:
    """Resolved input/output datasets for a single env."""

    def __init__(
        self,
        input_datasets: dict[str, Dataset],
        output_datasets: dict[str, Dataset],
        spark: SparkSession,
    ) -> None:
        self.input = _InputSide(input_datasets, spark)
        self.output = _OutputSide(output_datasets, spark)


class DatasetContext:
    """Env-aware access to resolved input/output datasets.

    Default run env:
        ctx.input.read("orders")
        ctx.output.write("main", df)

    Another env (requires app_name set):
        ctx("sandbox").input.read("orders")
    """

    def __init__(
        self,
        app_name: str,
        env: str,
        ymd: str,
        hms: str,
        spark: SparkSession,
        config: dict | None = None,
    ) -> None:
        self._app_name = app_name
        self._env = env
        self._ymd = ymd
        self._hms = hms
        self._spark = spark
        self._cache: dict[str, _EnvView] = {}
        if config is not None:
            self._cache[env] = self._resolve_env(config, env)

    @classmethod
    def from_merged_config(
        cls,
        config: dict,
        env: str,
        ymd: str,
        hms: str,
        spark: SparkSession | None = None,
    ) -> DatasetContext:
        """Build from an already-merged config (tests, notebooks with manual config)."""
        if spark is None:
            raise ValueError("spark is required")
        ctx = cls(app_name="", env=env, ymd=ymd, hms=hms, spark=spark, config=config)
        return ctx

    def __call__(self, env: str) -> _EnvView:
        if not self._app_name:
            raise RuntimeError(
                "cross-env access requires app_name (use DatasetContext with app_name from SparkAppBase)"
            )
        view = self._cache.get(env)
        if view is None:
            config = ConfigLoader.load(self._app_name, env)
            view = self._resolve_env(config, env)
            self._cache[env] = view
        return view

    @property
    def input(self) -> _InputSide:
        return self._default_view.input

    @property
    def output(self) -> _OutputSide:
        return self._default_view.output

    @property
    def _default_view(self) -> _EnvView:
        view = self._cache.get(self._env)
        if view is None:
            raise RuntimeError(f"no datasets resolved for env '{self._env}'")
        return view

    def _resolve_env(self, config: dict, env: str) -> _EnvView:
        resolve_ctx = ResolveContext(config=config, env=env, ymd=self._ymd, hms=self._hms)
        datasets = config.get("datasets") or {}
        input_datasets = self._resolve_group(datasets.get("input") or {}, resolve_ctx)
        output_datasets = self._resolve_group(datasets.get("output") or {}, resolve_ctx)
        return _EnvView(input_datasets, output_datasets, self._spark)

    @staticmethod
    def _resolve_group(specs: dict, context: ResolveContext) -> dict[str, Dataset]:
        resolved: dict[str, Dataset] = {}
        for name, spec in specs.items():
            if not isinstance(spec, dict):
                raise ValueError(f"dataset '{name}' spec must be a mapping, got {type(spec).__name__}")
            dataset_type = detect_type(spec)
            dataset_cls = get_dataset_class(dataset_type)
            resolved[name] = dataset_cls.from_spec(name, spec, context)
        return resolved
