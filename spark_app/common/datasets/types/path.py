from spark_app.common.datasets.models import Dataset, ResolveContext, apply_templates

_RESERVED_KEYS = {"type", "path"}


class PathDataset(Dataset):
    type = "path"
    default_format = "parquet"

    # read/write inherited from Dataset — generic format/options/partitionBy IO.

    @classmethod
    def from_spec(cls, name: str, spec: dict, context: ResolveContext) -> "PathDataset":
        path = spec.get("path")
        if not path:
            raise ValueError(f"dataset '{name}' path spec requires 'path'")

        relative_path = apply_templates(str(path), context).lstrip("/")
        location = f"{context.warehouse}/{relative_path}"
        metadata = {key: value for key, value in spec.items() if key not in _RESERVED_KEYS}

        return cls(name=name, location=location, metadata=metadata)
