from unittest.mock import MagicMock

import pytest


def test_read_delegates_to_dataset(merged_config, make_dataset_context, spark):
    ctx = make_dataset_context(merged_config)
    frame = MagicMock()
    spark.table.return_value = frame

    result = ctx.input.read("orders")

    assert result is frame
    spark.table.assert_called_once_with("iceberg.raw.orders")


def test_resolves_nested_catalog_table(make_dataset_context):
    config = {
        "catalog": {"name": "iceberg"},
        "datasets": {
            "warehouse": "s3a://local",
            "input": {
                "orders": {"type": "table", "table": "refined.sample.orders"},
            },
        },
    }
    ctx = make_dataset_context(config, kind="input")

    assert ctx["orders"].location == "iceberg.refined.sample.orders"


def test_resolves_path_and_table_datasets(merged_config, make_dataset_context, test_ymd):
    ctx = make_dataset_context(merged_config)

    assert ctx.input["orders"].type == "table"
    assert ctx.input["orders"].location == "iceberg.raw.orders"

    assert ctx.output["main"].type == "path"
    assert ctx.output["main"].location == f"s3a://local/mart/test_app/summary/ymd={test_ymd}"
    assert ctx.output["main"].metadata == {"format": "parquet", "mode": "overwrite"}


def test_table_with_path_location(merged_config, make_dataset_context, test_ymd):
    ctx = make_dataset_context(merged_config)

    assert ctx.input["impression"].type == "table"
    assert ctx.input["impression"].location == f"s3a://local/raw/events/impression/ymd={test_ymd}"
    assert ctx.input["impression"].metadata == {"format": "parquet", "catalog": "iceberg"}


def test_invalid_dataset_spec_raises(merged_config, make_dataset_context, copy_config_fn):
    config = copy_config_fn(merged_config)
    config["datasets"]["input"]["bad"] = {"topic": "events"}

    with pytest.raises(ValueError, match="requires 'type'"):
        make_dataset_context(config, kind="input")


def test_path_requires_warehouse(make_dataset_context):
    config = {
        "datasets": {
            "output": {"main": {"type": "path", "path": "mart/out"}},
        },
    }

    with pytest.raises(ValueError, match="datasets.warehouse is required"):
        make_dataset_context(config, kind="output")
