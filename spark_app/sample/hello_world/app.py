from spark_app.common.base import BaseSparkApp
from pyspark.sql import SparkSession

class HelloWorldApp(BaseSparkApp):
    def run(self, spark: SparkSession) -> None:
        spark.sql("SELECT 'Hello, World!'").show()

if __name__ == "__main__":
    app = HelloWorldApp()
    app.execute()