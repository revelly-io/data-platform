import pytest

from spark_app.common.config.loader import ConfigLoader


def test_load_merges_global_and_app_config(config_loader_root, merged_config, test_app_name, test_env):
    config = ConfigLoader.load(test_app_name, env=test_env)

    assert config == merged_config


def test_load_missing_app_raises(config_loader_root, test_env):
    with pytest.raises(FileNotFoundError, match="each app must provide config.yaml"):
        ConfigLoader.load("sample.no_such_app", env=test_env)


def test_load_missing_env_file_raises(config_loader_root, test_app_name, test_env):
    (config_loader_root / f".env.{test_env}").unlink()

    with pytest.raises(FileNotFoundError, match=r"Missing .*/\.env\.local"):
        ConfigLoader.load(test_app_name, env=test_env)


def test_load_missing_env_variable_raises(config_loader_root, test_app_name, monkeypatch):
    monkeypatch.delenv("MISSING_VAR", raising=False)
    global_path = config_loader_root / "config" / "local" / ConfigLoader.GLOBAL_CONFIG_FILE
    global_path.write_text(
        "spark:\n  configs:\n    spark.hadoop.fs.s3a.secret.key: ${MISSING_VAR}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Missing environment variable: MISSING_VAR"):
        ConfigLoader.load(test_app_name, env="local")


def test_load_resolves_env_placeholders(config_loader_root, test_app_name, monkeypatch):
    monkeypatch.setenv("S3A_ACCESS_KEY", "from-env")
    global_path = config_loader_root / "config" / "local" / ConfigLoader.GLOBAL_CONFIG_FILE
    global_path.write_text(
        "spark:\n  configs:\n    spark.hadoop.fs.s3a.access.key: ${S3A_ACCESS_KEY}\n",
        encoding="utf-8",
    )

    config = ConfigLoader.load(test_app_name, env="local")

    assert config["spark"]["configs"]["spark.hadoop.fs.s3a.access.key"] == "from-env"
