from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from spark_app.common.datasets.models import Dataset

if TYPE_CHECKING:
    from spark_app.common.bases.base import SparkAppBase
    from spark_app.common.bases.batch import SparkBatchAppBase

logger = logging.getLogger(__name__)


def _json(data: object) -> str:
    return json.dumps(data, ensure_ascii=False)


def _config_for_log(config: dict) -> dict:
    return {key: value for key, value in config.items() if key != "datasets"}


def _serialize_datasets(app: SparkBatchAppBase) -> dict:
    def serialize_group(group: dict[str, Dataset]) -> dict:
        return {name: {"type": ds.type, "location": ds.location, "metadata": ds.metadata} for name, ds in group.items()}

    return {
        "input": serialize_group(app.input.resolved),
        "output": serialize_group(app.output.resolved),
    }


def log_batch_startup(app: SparkBatchAppBase) -> None:
    logger.info(
        "Spark app | [%s] %s (ymd=%s, hms=%s)",
        app.env,
        app.app_name,
        app.ymd,
        app.hms,
    )
    logger.info("Extra args | %s", _json(app.extra_args))
    logger.info("Config | %s", _json(_config_for_log(app.config)))
    logger.info("Datasets | %s", _json(_serialize_datasets(app)))


def log_ops_startup(app: SparkAppBase) -> None:
    logger.info("Spark app | [%s] %s", app.env, app.app_name)
    logger.info("Extra args | %s", _json(app.extra_args))
    logger.info("Config | %s", _json(_config_for_log(app.config)))
