from collections.abc import Callable
from copy import deepcopy
from unittest.mock import MagicMock

import pytest
import yaml

from spark_app.common.app_factory import AppFactory
from spark_app.common.bases.base import SparkAppBase
from spark_app.common.config.loader import ConfigLoader
from spark_app.common.config.merge import deep_merge
from spark_app.common.datasets import DatasetContext

TEST_APP_NAME = "sample.test_app"
TEST_ENV = "local"
TEST_YMD = "2026-07-01"
TEST_HMS = "121212"


@pytest.fixture
def test_app_name() -> str:
    return TEST_APP_NAME


@pytest.fixture
def test_env() -> str:
    return TEST_ENV


@pytest.fixture
def test_ymd() -> str:
    return TEST_YMD


@pytest.fixture
def test_hms() -> str:
    return TEST_HMS


@pytest.fixture
def spark() -> MagicMock:
    return MagicMock()


@pytest.fixture
def global_config() -> dict:
    return {
        "catalog": {"name": "iceberg"},
        "datasets": {"warehouse": "s3a://localhost:9000/warehouse"},
        "spark": {
            "master": "local[*]",
            "configs": {"spark.sql.shuffle.partitions": "4"},
        },
    }


@pytest.fixture
def app_config() -> dict:
    return {
        "spark": {"configs": {"spark.sql.shuffle.partitions": "1"}},
        "datasets": {
            "input": {
                "orders": {"type": "table", "table": "raw.orders"},
                "impression": {
                    "type": "table",
                    "path": "landing/impression/ymd={ymd}",
                    "format": "parquet",
                },
            },
            "output": {
                "main": {
                    "type": "path",
                    "path": "mart/test_app/ymd={ymd}",
                    "format": "parquet",
                    "mode": "overwrite",
                },
            },
        },
    }


@pytest.fixture
def merged_config(global_config: dict, app_config: dict) -> dict:
    return deep_merge(global_config, app_config)


@pytest.fixture
def app_factory_argv() -> list[str]:
    return [
        "--app_name",
        TEST_APP_NAME,
        "--env",
        TEST_ENV,
        "--ymd",
        TEST_YMD,
        "--hms",
        TEST_HMS,
        "--foo",
        "bar",
    ]


@pytest.fixture
def stub_spark_app_class():
    class StubSparkApp(SparkAppBase):
        def run(self, spark) -> None:
            pass

    return StubSparkApp


@pytest.fixture
def test_app(stub_spark_app_class, merged_config: dict) -> SparkAppBase:
    return stub_spark_app_class(
        app_name=TEST_APP_NAME,
        env=TEST_ENV,
        ymd=TEST_YMD,
        hms=TEST_HMS,
        config=merged_config,
        extra_args={"foo": "bar"},
    )


@pytest.fixture
def config_loader_root(tmp_path, global_config: dict, app_config: dict, monkeypatch):
    global_dir = tmp_path / "config" / TEST_ENV
    global_dir.mkdir(parents=True)
    (global_dir / ConfigLoader.GLOBAL_CONFIG_FILE).write_text(yaml.dump(global_config))

    app_dir = tmp_path.joinpath(*TEST_APP_NAME.split("."))
    app_dir.mkdir(parents=True)
    (app_dir / ConfigLoader.CONFIG_FILE).write_text(yaml.dump(app_config))
    (app_dir / "app.py").write_text("# stub app module for layout validation\n")

    monkeypatch.setattr(ConfigLoader, "SPARK_APP_ROOT", tmp_path)
    monkeypatch.setattr(ConfigLoader, "GLOBAL_CONFIG_ROOT", tmp_path / "config")
    return tmp_path


@pytest.fixture
def make_dataset_context(spark: MagicMock) -> Callable[..., DatasetContext]:
    def _make(
        config: dict,
        *,
        env: str = TEST_ENV,
        ymd: str = TEST_YMD,
        hms: str = TEST_HMS,
    ) -> DatasetContext:
        return DatasetContext.from_merged_config(config, env=env, ymd=ymd, hms=hms, spark=spark)

    return _make


@pytest.fixture
def app_factory(app_factory_argv: list[str], config_loader_root, stub_spark_app_class, monkeypatch) -> AppFactory:
    monkeypatch.setattr(AppFactory, "_load_app_class", lambda self, app_name: stub_spark_app_class)
    return AppFactory(app_factory_argv)


def copy_config(config: dict) -> dict:
    return deepcopy(config)


@pytest.fixture
def copy_config_fn():
    return copy_config