import yaml


class PipelineConfig:
    """Thin wrapper around the YAML config."""

    def __init__(self, data: dict):
        self.pipeline = data.get("pipeline", {})
        self.source = data["source"]
        self.transformations = data.get("transformations", [])
        self.output_columns = data.get("output", {}).get("columns", [])
        self.validation_rules = data.get("validation", {}).get("rules", [])

    @classmethod
    def from_yaml(cls, path: str) -> "PipelineConfig":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls(data)
