import yaml


def load_config(
    path: str = 'pipeline_config.yaml' 
) -> dict:

    with open(path, "r") as f:
        return yaml.safe_load(f)