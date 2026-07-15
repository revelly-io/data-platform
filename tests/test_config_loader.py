import pytest

from spark_app.common.config.loader import ConfigLoader


def test_load_merges_global_and_app_config(config_loader_root, merged_config, test_app_name, test_env):
    config = ConfigLoader.load(test_app_name, env=test_env)

    assert config == merged_config


def test_load_missing_app_raises(config_loader_root, test_env):
    with pytest.raises(FileNotFoundError, match="each app must provide config.yaml"):
        ConfigLoader.load("sample.no_such_app", env=test_env)
