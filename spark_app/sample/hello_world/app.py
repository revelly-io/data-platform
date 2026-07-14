from pyspark.sql import SparkSession

from spark_app.common.bases.base import SparkAppBase


class HelloWorldApp(SparkAppBase):
    def run(self, spark: SparkSession) -> None:
        spark.sql("SELECT 'Hello, World!'").show()
