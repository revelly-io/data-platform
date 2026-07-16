from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_app.common.bases.base import SparkAppBase


class OrdersSummaryApp(SparkAppBase):
    """Read sample orders parquet from MinIO, aggregate by status, and show."""

    def run(self, spark: SparkSession) -> None:
        orders = self.input.read("orders")

        summary = (
            orders.groupBy("status")
            .agg(
                F.count("*").alias("order_count"),
                F.round(F.sum("amount"), 2).alias("total_amount"),
            )
            .orderBy("status")
        )

        self.logger.info("Orders source | rows=%s", orders.count())
        summary.show(truncate=False)
        self.logger.info("Summary | groups=%s", summary.count())
