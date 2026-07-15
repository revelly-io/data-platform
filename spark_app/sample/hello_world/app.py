from pyspark.sql import SparkSession

from spark_app.common.bases.base import SparkAppBase


class HelloWorldApp(SparkAppBase):
    def run(self, spark: SparkSession) -> None:
        self.logger.info("Hello, world!")
        df = spark.createDataFrame([(1, "hello"), (2, "world")], ["id", "msg"])
        self.logger.info("Spark smoke check | rows=%s", df.count())
