import pytest

from spark_app.common.app_factory import AppFactory


def test_build_app(app_factory, merged_config, stub_spark_app_class, test_app_name, test_env, test_ymd, test_hms):
    app = app_factory.build()

    assert isinstance(app, stub_spark_app_class)
    assert app._app_name == test_app_name
    assert app._env == test_env
    assert app._ymd == test_ymd
    assert app._hms == test_hms
    assert app._extra_args == {"foo": "bar"}
    assert app._config == merged_config


def test_build_batch_app_missing_ymd_hms_raises(
    config_loader_root, stub_spark_app_class, monkeypatch, test_app_name, test_env
):
    monkeypatch.setattr(AppFactory, "_load_app_class", lambda self, app_name: stub_spark_app_class)
    factory = AppFactory(["--app_name", test_app_name, "--env", test_env])

    with pytest.raises(ValueError, match="requires --ymd and --hms"):
        factory.build()


def test_build_ops_app_without_ymd_hms(config_loader_root, stub_spark_ops_app_class, monkeypatch, test_app_name, test_env):
    monkeypatch.setattr(AppFactory, "_load_app_class", lambda self, app_name: stub_spark_ops_app_class)
    factory = AppFactory(["--app_name", test_app_name, "--env", test_env, "--layer", "refined"])

    app = factory.build()

    assert isinstance(app, stub_spark_ops_app_class)
    assert app.extra_args == {"layer": "refined"}


def test_build_ops_app_with_ymd_hms_raises(
    config_loader_root, stub_spark_ops_app_class, monkeypatch, test_app_name, test_env, test_ymd, test_hms
):
    monkeypatch.setattr(AppFactory, "_load_app_class", lambda self, app_name: stub_spark_ops_app_class)
    factory = AppFactory(["--app_name", test_app_name, "--env", test_env, "--ymd", test_ymd, "--hms", test_hms])

    with pytest.raises(ValueError, match="does not accept --ymd/--hms"):
        factory.build()
