from unittest.mock import MagicMock, patch


def test_execute_runs_lifecycle(test_app):
    mock_spark = MagicMock()

    with (
        patch.object(test_app, "_build_spark", return_value=mock_spark),
        patch("spark_app.common.bases.base.log_app_startup") as log_startup,
        patch.object(test_app, "run") as run,
    ):
        test_app.execute()

    log_startup.assert_called_once_with(test_app)
    run.assert_called_once_with(mock_spark)
    mock_spark.stop.assert_called_once()
    assert test_app.datasets.input["orders"].location == "iceberg.raw.orders"
