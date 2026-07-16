import os
from pathlib import Path
from string import Template
from typing import Any

from dotenv import load_dotenv

from spark_app.common.config.merge import deep_merge, load_yaml


class ConfigLoader:
    """Loads and merges global + app config for a given env and app."""

    SPARK_APP_ROOT = Path(__file__).resolve().parents[2]
    REPO_ROOT = SPARK_APP_ROOT.parent
    GLOBAL_CONFIG_ROOT = SPARK_APP_ROOT / "config"

    CONFIG_FILE = "config.yaml"
    GLOBAL_CONFIG_FILE = "global-config.yaml"

    @classmethod
    def load(cls, app_name: str, env: str) -> dict:
        """Merge global-config for env with the app's config.yaml (app wins)."""
        env_file = cls.REPO_ROOT / f".env.{env}"
        if not env_file.is_file():
            raise FileNotFoundError(f"Missing {env_file} (copy .env.{env}.example → .env.{env})")
        load_dotenv(env_file)

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

        merged = deep_merge(load_yaml(global_config_path), load_yaml(app_config_path))
        return _expand_env(merged)

    @classmethod
    def app_dir(cls, app_name: str) -> Path:
        """Resolve a dotted app name (e.g. 'sample.orders_summary') to its package dir."""
        return cls.SPARK_APP_ROOT / Path(*app_name.split("."))


def _expand_env(config: dict[str, Any]) -> dict[str, Any]:
    def walk(node: Any) -> Any:
        if isinstance(node, dict):
            return {key: walk(value) for key, value in node.items()}
        if isinstance(node, str) and "$" in node:
            return _expand_string(node)
        return node

    return walk(config)


def _expand_string(value: str) -> str:
    try:
        return Template(value).substitute(os.environ)
    except KeyError as exc:
        raise ValueError(f"Missing environment variable: {exc.args[0]}") from None
