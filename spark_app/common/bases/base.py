import logging
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from spark_app.common.bases.logging import log_app_startup
from spark_app.common.datasets.context import DatasetContext


class SparkAppBase(ABC):
    def __init__(
        self,
        app_name: str,
        env: str,
        ymd: str,
        hms: str,
        config: dict | None = None,
        extra_args: dict | None = None,
    ) -> None:
        self._app_name = app_name
        self._env = env
        self._ymd = ymd
        self._hms = hms
        self._config = config or {}
        self._extra_args = extra_args or {}
        self._logger = logging.getLogger(type(self).__module__)
        self._input: DatasetContext | None = None
        self._output: DatasetContext | None = None

    @property
    def input(self) -> DatasetContext:
        if self._input is None:
            raise RuntimeError("input is not available until execute() builds it")
        return self._input

    @property
    def output(self) -> DatasetContext:
        if self._output is None:
            raise RuntimeError("output is not available until execute() builds it")
        return self._output

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    def _build_spark(self) -> SparkSession:
        spark_config = self._config.get("spark", {})
        master = spark_config.get("master", "local[*]")
        configs = spark_config.get("configs") or {}

        builder = SparkSession.builder.appName(self._app_name).master(master)
        for key, value in configs.items():
            builder = builder.config(key, str(value))

        return builder.getOrCreate()

    @abstractmethod
    def run(self, spark: SparkSession) -> None:
        pass

    def execute(self) -> None:
        spark = self._build_spark()
        self._input, self._output = DatasetContext.pair(
            app_name=self._app_name,
            env=self._env,
            ymd=self._ymd,
            hms=self._hms,
            spark=spark,
            config=self._config,
        )
        log_app_startup(self)
        try:
            self.run(spark)
        finally:
            spark.stop()
