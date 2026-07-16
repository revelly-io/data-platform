import argparse
import importlib
import inspect
from datetime import date, datetime
from pathlib import Path

from spark_app.common.bases.base import SparkAppBase
from spark_app.common.bases.batch import SparkBatchAppBase
from spark_app.common.bases.ops import SparkOpsAppBase
from spark_app.common.config.loader import ConfigLoader

# Root package name for imports. Change this if the top-level package is renamed.
SPARK_APP_PACKAGE = "spark_app"
APP_MODULE = "app"

RESERVED_KEYS = {"app_name", "env", "ymd", "hms"}
ABSTRACT_BASES = (SparkAppBase, SparkBatchAppBase, SparkOpsAppBase)


def _parse_ymd(value: str) -> str:
    try:
        date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("ymd must be YYYY-MM-DD")
    return value


def _parse_hms(value: str) -> str:
    try:
        datetime.strptime(value, "%H%M%S")
    except ValueError:
        raise argparse.ArgumentTypeError("hms must be HHMMSS")
    return value


class AppFactory:
    """Parses CLI arguments and assembles a runnable SparkAppBase instance.

    Discovers both SparkBatchAppBase apps (ymd/hms-partitioned) and SparkOpsAppBase
    apps (one-off/maintenance) by app_name. --ymd/--hms are required only for
    SparkBatchAppBase apps.
    """

    def __init__(self, argv: list[str] | None = None):
        self._known, self._extra_args = self._parse_args(argv)

    def build(self) -> SparkAppBase:
        app_directory = ConfigLoader.app_dir(self._known.app_name)
        self._validate_app_module(app_directory, self._known.app_name)

        app_cls = self._load_app_class(self._known.app_name)

        kwargs = {
            "app_name": self._known.app_name,
            "env": self._known.env,
            "extra_args": self._extra_args,
        }
        if issubclass(app_cls, SparkBatchAppBase):
            if self._known.ymd is None or self._known.hms is None:
                raise ValueError(f"{self._known.app_name} requires --ymd and --hms (SparkBatchAppBase app)")
            kwargs["ymd"] = self._known.ymd
            kwargs["hms"] = self._known.hms
        elif self._known.ymd is not None or self._known.hms is not None:
            raise ValueError(f"{self._known.app_name} does not accept --ymd/--hms (not a SparkBatchAppBase app)")

        return app_cls(**kwargs)

    def _parse_args(self, argv: list[str] | None):
        parser = argparse.ArgumentParser()
        parser.add_argument("--app_name", required=True)
        parser.add_argument("--env", required=True, choices=["local", "homelab", "aws"])
        parser.add_argument("--ymd", required=False, default=None, type=_parse_ymd)
        parser.add_argument("--hms", required=False, default=None, type=_parse_hms)

        known, unknown = parser.parse_known_args(argv)
        if len(unknown) % 2 != 0:
            raise ValueError(f"Unpaired extra arguments: {unknown}")

        extra_args = dict(zip((k.lstrip("-") for k in unknown[0::2]), unknown[1::2]))

        clashed = RESERVED_KEYS & extra_args.keys()
        if clashed:
            raise ValueError(f"extra args clash with reserved args: {clashed}")

        return known, extra_args

    def _validate_app_module(self, app_directory: Path, app_name: str) -> None:
        app_py = app_directory / f"{APP_MODULE}.py"

        if not app_directory.is_dir():
            raise FileNotFoundError(
                f"App package not found: {app_directory} (expected {SPARK_APP_PACKAGE}/{'/'.join(app_name.split('.'))}/)"
            )
        if not app_py.is_file():
            raise FileNotFoundError(f"Missing required file: {app_py} (each app must provide {APP_MODULE}.py)")

    def _load_app_class(self, app_name: str) -> type[SparkAppBase]:
        module_path = f"{SPARK_APP_PACKAGE}.{app_name}.{APP_MODULE}"
        module = importlib.import_module(module_path)
        classes = [
            obj
            for _, obj in inspect.getmembers(module, inspect.isclass)
            if issubclass(obj, SparkAppBase) and obj not in ABSTRACT_BASES
        ]
        if len(classes) != 1:
            raise ImportError(f"{module_path} must define exactly one SparkAppBase subclass, found {len(classes)}")
        return classes[0]
