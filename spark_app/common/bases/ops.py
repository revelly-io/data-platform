from spark_app.common.bases.base import SparkAppBase
from spark_app.common.bases.logging import log_ops_startup


class SparkOpsAppBase(SparkAppBase):
    """Base for ops/maintenance apps: no ymd/hms/DatasetContext, config.yaml optional.

    Sibling of SparkBatchAppBase. Used for one-off or maintenance-style Spark apps
    (catalog DDL apply, Iceberg compaction, ...) that don't operate over a
    ymd/hms-partitioned dataset. Override load_overlay_config() if a concrete app
    needs its own config.yaml merged on top of the global config.
    """

    def _log_startup(self) -> None:
        log_ops_startup(self)
