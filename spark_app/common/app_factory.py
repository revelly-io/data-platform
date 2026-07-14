import argparse
import importlib
import inspect
from pathlib import Path

import yaml

from spark_app.common.bases.base import SparkAppBase

# Root package name for imports. Change this if the top-level package is renamed.
SPARK_APP_PACKAGE = "spark_app"
APP_MODULE = "app"
CONFIG_FILE = "config.yaml"
SPARK_APP_ROOT = Path(__file__).resolve().parent.parent

RESERVED_KEYS = {"app_name", "env", "ymd", "hms"}


class AppFactory:
    """Parses CLI arguments and assembles a runnable SparkAppBase instance."""

    def __init__(self, argv: list[str] | None = None):
        self._known, self._extra_args = self._parse_args(argv)

    def build(self) -> SparkAppBase:
        app_dir = self._resolve_app_dir(self._known.app_name)
        self._validate_app_layout(app_dir, self._known.app_name)

        app_cls = self._load_app_class(self._known.app_name)
        config = self._load_config(app_dir)
        return app_cls(
            app_name=self._known.app_name,
            env=self._known.env,
            ymd=self._known.ymd,
            hms=self._known.hms,
            config=config,
            args=self._extra_args,
        )

    def _parse_args(self, argv: list[str] | None):
        parser = argparse.ArgumentParser()
        parser.add_argument("--app_name", required=True)
        parser.add_argument("--env", required=True, choices=["local", "sandbox"])
        parser.add_argument("--ymd", required=True)
        parser.add_argument("--hms", required=True)

        known, unknown = parser.parse_known_args(argv)
        if len(unknown) % 2 != 0:
            raise ValueError(f"Unpaired extra arguments: {unknown}")

        extra_args = dict(zip((k.lstrip("-") for k in unknown[0::2]), unknown[1::2]))

        clashed = RESERVED_KEYS & extra_args.keys()
        if clashed:
            raise ValueError(f"extra args clash with reserved args: {clashed}")

        return known, extra_args

    def _resolve_app_dir(self, app_name: str) -> Path:
        return SPARK_APP_ROOT / Path(*app_name.split("."))

    def _validate_app_layout(self, app_dir: Path, app_name: str) -> None:
        app_py = app_dir / f"{APP_MODULE}.py"
        config_yaml = app_dir / CONFIG_FILE

        if not app_dir.is_dir():
            raise FileNotFoundError(
                f"App package not found: {app_dir} "
                f"(expected {SPARK_APP_PACKAGE}/{'/'.join(app_name.split('.'))}/)"
            )
        if not app_py.is_file():
            raise FileNotFoundError(
                f"Missing required file: {app_py} "
                f"(each app must provide {APP_MODULE}.py)"
            )
        if not config_yaml.is_file():
            raise FileNotFoundError(
                f"Missing required file: {config_yaml} "
                f"(each app must provide {CONFIG_FILE})"
            )

    def _load_app_class(self, app_name: str) -> type[SparkAppBase]:
        module_path = f"{SPARK_APP_PACKAGE}.{app_name}.{APP_MODULE}"
        module = importlib.import_module(module_path)
        classes = [
            obj for _, obj in inspect.getmembers(module, inspect.isclass)
            if issubclass(obj, SparkAppBase) and obj is not SparkAppBase
        ]
        if len(classes) != 1:
            raise ImportError(
                f"{module_path} must define exactly one SparkAppBase subclass, "
                f"found {len(classes)}"
            )
        return classes[0]

    def _load_config(self, app_dir: Path) -> dict:
        with open(app_dir / CONFIG_FILE) as f:
            return yaml.safe_load(f) or {}
