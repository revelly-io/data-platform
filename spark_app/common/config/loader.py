from pathlib import Path

from spark_app.common.config.merge import deep_merge, load_yaml


class ConfigLoader:
    """Loads and merges global + app config for a given env and app."""

    SPARK_APP_ROOT = Path(__file__).resolve().parents[2]
    GLOBAL_CONFIG_ROOT = SPARK_APP_ROOT / "config"

    CONFIG_FILE = "config.yaml"
    GLOBAL_CONFIG_FILE = "global-config.yaml"

    @classmethod
    def app_dir(cls, app_name: str) -> Path:
        """Resolve a dotted app name (e.g. 'sample.hello_world') to its package dir."""
        return cls.SPARK_APP_ROOT / Path(*app_name.split("."))

    @classmethod
    def load(cls, app_name: str, env: str) -> dict:
        """Merge global-config for env with the app's config.yaml (app wins)."""
        global_config_path = cls.GLOBAL_CONFIG_ROOT / env / cls.GLOBAL_CONFIG_FILE
        if not global_config_path.is_file():
            raise FileNotFoundError(
                f"Missing required file: {global_config_path} (each env must provide {cls.GLOBAL_CONFIG_FILE})"
            )

        app_config_path = cls.app_dir(app_name) / cls.CONFIG_FILE
        if not app_config_path.is_file():
            raise FileNotFoundError(
                f"Missing required file: {app_config_path} (each app must provide {cls.CONFIG_FILE})"
            )

        global_config = load_yaml(global_config_path)
        app_config = load_yaml(app_config_path)
        return deep_merge(global_config, app_config)
