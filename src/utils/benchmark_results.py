"""Benchmark results management and output."""

import csv
import json
from pathlib import Path
from typing import Any, Dict, List

from src.utils.logger import get_logger

logger = get_logger("benchmark")


def ensure_output_dir(path: Path) -> None:
    """Create parent directories for output file if needed.

    Args:
        path: Path object for target file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)


def get_output_path(
    base_output: str,
    processing_mode: str,
) -> Path:
    """Determine output file path based on processing mode.

    Args:
        base_output: Base output path.
        processing_mode: Processing mode (file_loop or full_batch).

    Returns:
        Path object with mode-specific filename.
    """
    output_path = Path(base_output)
    stem = output_path.stem
    suffix = output_path.suffix
    output_path = output_path.parent / f"{stem}_{processing_mode}{suffix}"
    return output_path


def write_results(
    results: List[Dict[str, Any]],
    output_path: Path,
) -> None:
    """Write benchmark results to CSV file.

    Args:
        results: List of result dictionaries.
        output_path: Target file path.
    """
    ensure_output_dir(output_path)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)

    logger.info("Results written to %s", output_path)


def print_best_trial(results: List[Dict[str, Any]]) -> None:
    """Print best trial by execution time.

    Args:
        results: List of result dictionaries.
    """
    successful = [r for r in results if r["status"] == "ok"]
    if not successful:
        logger.warning("No successful trials found")
        return

    best = sorted(successful, key=lambda item: item["seconds"])[0]
    logger.info("Best trial: %s", json.dumps(best))
