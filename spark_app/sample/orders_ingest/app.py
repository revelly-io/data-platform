from pyspark.sql import SparkSession

from spark_app.common.bases.base import SparkAppBase


class OrdersIngestApp(SparkAppBase):
    """Read raw seed parquet from MinIO and append to refined Iceberg table."""

    def run(self, spark: SparkSession) -> None:
        orders = self.input.read("orders_seed")
        row_count = orders.count()
        self.output.write("orders", orders)
        self.logger.info("Ingested orders | rows=%s → refined.sample.orders", row_count)
