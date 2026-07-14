from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

class BaseSparkApp(ABC):

    def __init__(self, env: str = "local") -> SparkSession:
        self._env = env

    def _build_spark(self) -> SparkSession:
        builder = (
            SparkSession.builder
            .appName(self.__class__.__name__)
            .master("local[*]")
        )

        return builder.getOrCreate()

    @abstractmethod
    def run(self, spark: SparkSession) -> None:
        pass

    def execute(self) -> None:
        spark = self._build_spark()
        try:
            self.run(spark)
        finally:
            spark.stop()