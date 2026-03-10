"""Configuration loading from YAML files."""

from pathlib import Path
from typing import Any, Dict

import yaml


def load_config(path: str = "pipeline_config.yaml") -> Dict[str, Any]:
    """Load pipeline configuration from YAML file.

    Args:
        path: Path to YAML configuration file.

    Returns:
        Configuration dictionary.

    Raises:
        FileNotFoundError: If configuration file does not exist.
        yaml.YAMLError: If YAML parsing fails.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with config_path.open("r") as file:
        return yaml.safe_load(file)