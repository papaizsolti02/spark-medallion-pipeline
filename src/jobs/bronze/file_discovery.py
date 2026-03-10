"""File discovery utilities for raw data ingestion."""

import os
from typing import List, Optional


def list_parquet_files(raw_path: str, file_limit: Optional[int] = None) -> List[str]:
    """List parquet files in raw directory.

    Args:
        raw_path: Path to search for parquet files.
        file_limit: Optional maximum number of files to return.

    Returns:
        Sorted list of parquet filenames.
    """
    files = sorted(
        [f for f in os.listdir(raw_path) if f.endswith(".parquet")]
    )
    return files[:file_limit] if file_limit else files


def get_file_paths(raw_path: str, filenames: List[str]) -> List[str]:
    """Convert filenames to full file paths.

    Args:
        raw_path: Base directory path.
        filenames: List of filenames.

    Returns:
        List of full file paths.
    """
    return [os.path.join(raw_path, filename) for filename in filenames]
