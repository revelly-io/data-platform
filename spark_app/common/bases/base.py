import logging
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from spark_app.common.config.loader import ConfigLoader
from spark_app.common.config.merge import deep_merge, expand_env
from spark_app.common.spark_session import build_spark_session


class SparkAppBase(ABC):
    def __init__(
        self,
        app_name: str,
        env: str,
        config: dict | None = None,
        extra_args: dict | None = None,
    ) -> None:
        self._app_name = app_name
        self._env = env
        self._extra_args = extra_args or {}
        self._config = config if config is not None else self._load_config()
        self._logger = logging.getLogger(type(self).__module__)

    @property
    def app_name(self) -> str:
        return self._app_name

    @property
    def env(self) -> str:
        return self._env

    @property
    def config(self) -> dict:
        return self._config

    @property
    def extra_args(self) -> dict:
        return self._extra_args

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    def load_overlay_config(self) -> dict:
        """Hook: config to merge on top of the global config (app wins). Default: none."""
        return {}

    def _load_config(self) -> dict:
        global_config = ConfigLoader.load_global(self._env)  # already ${VAR}-expanded
        overlay = expand_env(self.load_overlay_config())
        return deep_merge(global_config, overlay)

    def _build_spark(self) -> SparkSession:
        return build_spark_session(self._app_name, self._config)

    def _prepare(self, spark: SparkSession) -> None:
        """Hook: called after the Spark session is built, before run(). Default: no-op."""

    def _log_startup(self) -> None:
        """Hook: called right before run(). Default: no-op."""

    @abstractmethod
    def run(self, spark: SparkSession) -> None:
        pass

    def execute(self) -> None:
        spark = self._build_spark()
        try:
            self._prepare(spark)
            self._log_startup()
            self.run(spark)
        finally:
            spark.stop()
