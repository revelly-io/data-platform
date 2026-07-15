from pyspark.sql import SparkSession

from spark_app.common.bases.base import SparkAppBase


class HelloWorldApp(SparkAppBase):
    def run(self, spark: SparkSession) -> None:
        partitions = spark.conf.get("spark.sql.shuffle.partitions")
        warehouse = self._config.get("datasets", {}).get("warehouse")
        self.logger.info(
            "Merged config applied | spark.sql.shuffle.partitions=%s datasets.warehouse=%s",
            partitions,
            warehouse,
        )
        spark.sql("SELECT 'Hello, World!' AS message").show()
