"""Notebook-only Spark helpers. Not part of spark_app."""

from __future__ import annotations

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from spark_app.common.config.loader import ConfigLoader, _expand_env
from spark_app.common.config.merge import load_yaml


def load_global_config(env: str = "local") -> dict:
    env_file = ConfigLoader.REPO_ROOT / f".env.{env}"
    if not env_file.is_file():
        raise FileNotFoundError(f"Missing {env_file} — copy .env.{env}.example")
    load_dotenv(env_file)

    global_path = ConfigLoader.GLOBAL_CONFIG_ROOT / env / ConfigLoader.GLOBAL_CONFIG_FILE
    if not global_path.is_file():
        raise FileNotFoundError(f"Missing {global_path}")

    return _expand_env(load_yaml(global_path))


def build_spark(config: dict, app_name: str) -> SparkSession:
    spark_cfg = config.get("spark", {})
    builder = SparkSession.builder.appName(app_name).master(spark_cfg.get("master", "local[*]"))
    for key, value in (spark_cfg.get("configs") or {}).items():
        builder = builder.config(key, str(value))
    return builder.getOrCreate()


def get_local_spark(app_name: str = "notebooks") -> SparkSession:
    return build_spark(load_global_config("local"), app_name)
