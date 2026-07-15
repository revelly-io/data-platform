from spark_app.common.datasets.models import Dataset
from spark_app.common.datasets.types.path import PathDataset
from spark_app.common.datasets.types.table import TableDataset

# Dataset type registry — add a new dataset `type` here.
# 1. Implement spark_app/common/datasets/types/<type>.py:
#    a Dataset subclass with `from_spec()` (resolution) and `read`/`write` (IO).
# 2. Import the class above and add an entry keyed by the yaml `type` value.
_REGISTRY: dict[str, type[Dataset]] = {
    "path": PathDataset,
    "table": TableDataset,
}


def detect_type(spec: dict) -> str:
    dataset_type = spec.get("type")
    if not dataset_type:
        raise ValueError(f"dataset spec requires 'type': {spec}")
    return str(dataset_type)


def get_dataset_class(dataset_type: str) -> type[Dataset]:
    try:
        return _REGISTRY[dataset_type]
    except KeyError as exc:
        registered = ", ".join(sorted(_REGISTRY)) or "(none)"
        raise ValueError(f"no dataset type registered for '{dataset_type}' (registered: {registered})") from exc
