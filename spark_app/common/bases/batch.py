from pyspark.sql import SparkSession

from spark_app.common.bases.base import SparkAppBase
from spark_app.common.bases.logging import log_batch_startup
from spark_app.common.config.loader import ConfigLoader
from spark_app.common.config.merge import load_yaml
from spark_app.common.datasets.context import DatasetContext


class SparkBatchAppBase(SparkAppBase):
    """Base for ETL batch apps: ymd/hms-partitioned, DatasetContext-backed, requires config.yaml."""

    def __init__(
        self,
        app_name: str,
        env: str,
        ymd: str,
        hms: str,
        config: dict | None = None,
        extra_args: dict | None = None,
    ) -> None:
        self._ymd = ymd
        self._hms = hms
        self._input: DatasetContext | None = None
        self._output: DatasetContext | None = None
        super().__init__(app_name, env, config=config, extra_args=extra_args)

    @property
    def ymd(self) -> str:
        return self._ymd

    @property
    def hms(self) -> str:
        return self._hms

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

    def load_overlay_config(self) -> dict:
        app_config_path = ConfigLoader.app_dir(self.app_name) / ConfigLoader.CONFIG_FILE
        if not app_config_path.is_file():
            raise FileNotFoundError(
                f"Missing required file: {app_config_path} (each app must provide {ConfigLoader.CONFIG_FILE})"
            )
        return load_yaml(app_config_path)

    def _prepare(self, spark: SparkSession) -> None:
        self._input, self._output = DatasetContext.pair(
            app_name=self.app_name,
            env=self.env,
            ymd=self._ymd,
            hms=self._hms,
            spark=spark,
            config=self.config,
        )

    def _log_startup(self) -> None:
        log_batch_startup(self)
