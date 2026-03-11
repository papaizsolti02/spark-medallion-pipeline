"""Benchmark configuration and trial generation utilities."""

import json
from itertools import product
from typing import Any, Dict, List


def parse_int_list(value: str) -> List[int]:
    """Parse comma-separated integers from string.

    Args:
        value: Comma-separated integer string.

    Returns:
        List of integers.
    """
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def parse_str_list(value: str) -> List[str]:
    """Parse comma-separated strings from string.

    Args:
        value: Comma-separated string.

    Returns:
        List of strings.
    """
    return [item.strip() for item in value.split(",") if item.strip()]


def build_trials(grid: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
    """Generate all combinations of configuration parameters.

    Args:
        grid: Dictionary mapping parameter names to lists of values.

    Returns:
        List of trial dictionaries with all combinations.
    """
    keys = list(grid.keys())
    trials = []
    for combo in product(*(grid[key] for key in keys)):
        trials.append(dict(zip(keys, combo)))
    return trials


def log_trial_params(trial_num: int, total: int, trial: Dict[str, Any]) -> None:
    """Log trial parameters in JSON format.

    Args:
        trial_num: Current trial number.
        total: Total number of trials.
        trial: Trial configuration dictionary.
    """
    print(f"Trial {trial_num}/{total}: {json.dumps(trial)}")
