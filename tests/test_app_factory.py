def test_build_app(app_factory, merged_config, stub_spark_app_class, test_app_name, test_env, test_ymd, test_hms):
    app = app_factory.build()

    assert isinstance(app, stub_spark_app_class)
    assert app._app_name == test_app_name
    assert app._env == test_env
    assert app._ymd == test_ymd
    assert app._hms == test_hms
    assert app._extra_args == {"foo": "bar"}
    assert app._config == merged_config
