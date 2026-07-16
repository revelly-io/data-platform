from unittest.mock import MagicMock, patch


def test_load_overlay_config_defaults_to_empty(stub_spark_ops_app_class, global_config):
    app = stub_spark_ops_app_class(app_name="catalog.ddl", env="local", config=global_config)

    assert app.load_overlay_config() == {}
    assert app.config == global_config


def test_ops_app_has_no_dataset_context(stub_spark_ops_app_class, global_config):
    app = stub_spark_ops_app_class(app_name="catalog.ddl", env="local", config=global_config)

    assert not hasattr(app, "input")
    assert not hasattr(app, "output")


def test_execute_runs_lifecycle_without_datasets(stub_spark_ops_app_class, global_config):
    app = stub_spark_ops_app_class(
        app_name="catalog.ddl",
        env="local",
        config=global_config,
        extra_args={"layer": "refined"},
    )
    mock_spark = MagicMock()

    with (
        patch.object(app, "_build_spark", return_value=mock_spark),
        patch("spark_app.common.bases.ops.log_ops_startup") as log_startup,
        patch.object(app, "run") as run,
    ):
        app.execute()

    log_startup.assert_called_once_with(app)
    run.assert_called_once_with(mock_spark)
    mock_spark.stop.assert_called_once()
