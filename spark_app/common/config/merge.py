import os
from pathlib import Path
from string import Template
from typing import Any

import yaml


def load_yaml(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def expand_env(config: dict[str, Any]) -> dict[str, Any]:
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
