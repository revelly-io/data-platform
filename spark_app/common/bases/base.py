import json
import logging
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


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
        logger.info(
            "Spark app context | app_name=%s env=%s ymd=%s hms=%s config=%s extra_args=%s",
            self._app_name,
            self._env,
            self._ymd,
            self._hms,
            json.dumps(self._config, ensure_ascii=False),
            json.dumps(self._extra_args, ensure_ascii=False),
        )
        spark = self._build_spark()
        try:
            self.run(spark)
        finally:
            spark.stop()
