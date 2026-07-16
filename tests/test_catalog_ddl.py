from unittest.mock import MagicMock

import pytest

from spark_app.catalog.ddl import app as ddl_app
from spark_app.catalog.ddl.app import CatalogDdlApp


@pytest.fixture
def ddl_root(tmp_path, monkeypatch):
    root = tmp_path / "ddl"
    (root / "refined" / "sample").mkdir(parents=True)
    (root / "refined" / "sample" / "orders.sql").write_text(
        "CREATE TABLE IF NOT EXISTS iceberg.refined.sample.orders (id bigint) "
        "USING iceberg LOCATION '${warehouse}/refined/sample/orders';"
    )
    monkeypatch.setattr(ddl_app, "DDL_ROOT", root)
    return root


def test_run_applies_ddl(ddl_root):
    config = {"catalog": {"name": "iceberg"}, "datasets": {"warehouse": "s3a://local"}}
    app = CatalogDdlApp(
        app_name="catalog.ddl",
        env="local",
        config=config,
        extra_args={"layer": "refined", "domain": "sample", "table": "orders"},
    )
    spark = MagicMock()

    app.run(spark)

    executed = [call.args[0] for call in spark.sql.call_args_list]
    assert "CREATE NAMESPACE IF NOT EXISTS iceberg.refined" in executed
    assert "CREATE NAMESPACE IF NOT EXISTS iceberg.refined.sample" in executed
    assert any("s3a://local/refined/sample/orders" in stmt for stmt in executed)


def test_run_drops_table(ddl_root):
    config = {"catalog": {"name": "iceberg"}}
    app = CatalogDdlApp(
        app_name="catalog.ddl",
        env="local",
        config=config,
        extra_args={"layer": "refined", "domain": "sample", "table": "orders", "drop": "true"},
    )
    spark = MagicMock()

    app.run(spark)

    spark.sql.assert_called_once_with("DROP TABLE IF EXISTS iceberg.refined.sample.orders")


def test_run_drop_requires_table(ddl_root):
    config = {"catalog": {"name": "iceberg"}}
    app = CatalogDdlApp(
        app_name="catalog.ddl",
        env="local",
        config=config,
        extra_args={"drop": "true"},
    )
    spark = MagicMock()

    with pytest.raises(ValueError, match="--drop requires"):
        app.run(spark)
