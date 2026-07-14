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
        args: dict | None = None,
    ) -> None:
        self._app_name = app_name
        self._env = env
        self._ymd = ymd
        self._hms = hms
        self._config = config or {}
        self._args = args or {}

    def _build_spark(self) -> SparkSession:
        builder = (
            SparkSession.builder
            .appName(f"{self._app_name}")
            .master("local[*]")
        )

        return builder.getOrCreate()

    @abstractmethod
    def run(self, spark: SparkSession) -> None:
        pass

    def execute(self) -> None:
        logger.info(
            "Spark app context | app_name=%s env=%s ymd=%s hms=%s config=%s args=%s",
            self._app_name,
            self._env,
            self._ymd,
            self._hms,
            json.dumps(self._config, ensure_ascii=False),
            json.dumps(self._args, ensure_ascii=False),
        )
        spark = self._build_spark()
        try:
            self.run(spark)
        finally:
            spark.stop()
