"""JVM process management utilities."""

import subprocess
import time
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger("benchmark")


def kill_java_processes() -> None:
    """Terminate lingering Java processes from Spark.

    Used for cleanup between benchmark trials to release resources.
    """
    try:
        subprocess.run(["pkill", "-9", "java"], timeout=5, capture_output=True)
        logger.info("Killed lingering Java processes")
    except Exception as exc:
        logger.warning("Failed to kill Java processes: %s", exc)


def recover_jvm_crash() -> None:
    """Perform cleanup and wait after JVM crash.

    Attempts to kill lingering processes and waits for system stabilization.
    """
    logger.warning("Attempting JVM recovery after crash")
    kill_java_processes()
    time.sleep(5)
    logger.info("JVM recovery complete, resuming trials")


def check_jvm_crash(execution_error: Optional[Exception]) -> bool:
    """Determine if error is due to JVM crash.

    Args:
        execution_error: Exception from trial execution.

    Returns:
        True if error indicates JVM crash.
    """
    if execution_error is None:
        return False

    error_str = str(execution_error)
    error_type = str(type(execution_error))
    return "Py4JNetworkError" in error_type or "ConnectionRefused" in error_str
