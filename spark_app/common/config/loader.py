from pathlib import Path

from dotenv import load_dotenv

from spark_app.common.config.merge import deep_merge, expand_env, load_yaml


class ConfigLoader:
    """Loads and merges global + app config for a given env and app."""

    SPARK_APP_ROOT = Path(__file__).resolve().parents[2]
    REPO_ROOT = SPARK_APP_ROOT.parent
    GLOBAL_CONFIG_ROOT = SPARK_APP_ROOT / "config"

    CONFIG_FILE = "config.yaml"
    GLOBAL_CONFIG_FILE = "global-config.yaml"

    @classmethod
    def load_global(cls, env: str) -> dict:
        """Load the .env.{env} file and this env's global-config.yaml, ${VAR}-expanded.

        Self-contained (safe to call directly, e.g. from notebooks) — expansion only
        substitutes from os.environ, so expanding here vs. after merging with an app
        overlay produces the same result either way.
        """
        env_file = cls.REPO_ROOT / f".env.{env}"
        if not env_file.is_file():
            raise FileNotFoundError(f"Missing {env_file} (copy .env.{env}.example → .env.{env})")
        load_dotenv(env_file)

        global_config_path = cls.GLOBAL_CONFIG_ROOT / env / cls.GLOBAL_CONFIG_FILE
        if not global_config_path.is_file():
            raise FileNotFoundError(
                f"Missing required file: {global_config_path} (each env must provide {cls.GLOBAL_CONFIG_FILE})"
            )

        return expand_env(load_yaml(global_config_path))

    @classmethod
    def load(cls, app_name: str, env: str) -> dict:
        """Merge global-config for env with the app's config.yaml (app wins)."""
        global_config = cls.load_global(env)

        app_config_path = cls.app_dir(app_name) / cls.CONFIG_FILE
        if not app_config_path.is_file():
            raise FileNotFoundError(
                f"Missing required file: {app_config_path} (each app must provide {cls.CONFIG_FILE})"
            )

        return deep_merge(global_config, expand_env(load_yaml(app_config_path)))

    @classmethod
    def app_dir(cls, app_name: str) -> Path:
        """Resolve a dotted app name (e.g. 'sample.orders_summary') to its package dir."""
        return cls.SPARK_APP_ROOT / Path(*app_name.split("."))
