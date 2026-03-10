"""Benchmark suite for Spark configuration tuning."""

import argparse
from time import perf_counter
from typing import Any, Dict, List, Optional

from src.core.config_loader import load_config
from src.core.spark import create_spark
from src.jobs.bronze.raw_to_bronze import run as raw_to_bronze
from src.utils.benchmark_results import get_output_path, print_best_trial, write_results
from src.utils.benchmark_utils import (
    build_trials,
    parse_int_list,
    parse_str_list,
)
from src.utils.jvm_manager import check_jvm_crash, recover_jvm_crash
from src.utils.logger import get_logger

logger = get_logger("benchmark")


def run_benchmark(
    trials: List[Dict[str, Any]],
    config: Dict[str, Any],
    processing_mode: str,
    file_limit: Optional[int],
) -> List[Dict[str, Any]]:
    """Execute all benchmark trials.

    Args:
        trials: List of trial configurations.
        config: Pipeline configuration.
        processing_mode: Processing mode (file_loop or full_batch).
        file_limit: Maximum files to process per trial.

    Returns:
        List of result dictionaries.
    """
    results: List[Dict[str, Any]] = []

    for idx, trial in enumerate(trials, 1):
        logger.info("Trial %d/%d: %s", idx, len(trials), trial)
        trial_start = perf_counter()
        spark = None
        status = "ok"
        error = ""

        try:
            spark = create_spark(
                app_name=f"spark-benchmark-{idx}",
                shuffle_partitions=int(trial["shuffle_partitions"]),
                cores=int(trial["cores"]),
                memory=str(trial["executor_memory"]),
                driver_memory=str(trial["driver_memory"]),
                max_partition_bytes=str(trial["max_partition_bytes"]),
            )

            result = raw_to_bronze(
                spark,
                config,
                processing_mode=processing_mode,
                coalesce_n=int(trial["coalesce_n"]),
                compression=str(trial["compression"]),
                file_limit=file_limit,
                logger=logger,
            )
            files_processed = result.get("files_processed", 0)
        except Exception as exc:
            status = "failed"
            files_processed = 0
            error = str(exc)
            logger.exception("Trial failed")

            if check_jvm_crash(exc):
                recover_jvm_crash()
        finally:
            if spark is not None:
                try:
                    spark.stop()
                except Exception as stop_exc:
                    logger.warning("Failed to stop Spark: %s", stop_exc)
                    if check_jvm_crash(stop_exc):
                        recover_jvm_crash()

        elapsed = round(perf_counter() - trial_start, 2)
        results.append(
            {
                **trial,
                "processing_mode": processing_mode,
                "status": status,
                "seconds": elapsed,
                "files_processed": files_processed,
                "error": error,
            }
        )
        logger.info("Trial %d finished in %ds (%s)", idx, elapsed, status)

    return results


def main() -> None:
    """Main benchmark entry point."""
    parser = argparse.ArgumentParser(
        description="Benchmark Spark configuration combinations for raw->bronze ingestion."
    )
    parser.add_argument("--config", default="configs/pipeline_config.yaml")
    parser.add_argument("--shuffle-partitions", default=None)
    parser.add_argument("--coalesce", default=None)
    parser.add_argument("--driver-memory", default=None)
    parser.add_argument("--executor-memory", default=None)
    parser.add_argument("--cores", default=None)
    parser.add_argument("--max-partition-bytes", default=None)
    parser.add_argument("--compression", default=None)
    parser.add_argument(
        "--processing-mode",
        default=None,
        choices=["full_batch", "file_loop"],
    )
    parser.add_argument("--file-limit", type=int, default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    benchmark_config = config.get("benchmark", {})

    # Get configuration from CLI args or config file
    shuffle_partitions = args.shuffle_partitions or str(
        benchmark_config.get("shuffle_partitions", "12,24")
    )
    coalesce = args.coalesce or str(benchmark_config.get("coalesce", "4,8"))
    driver_memory = args.driver_memory or str(
        benchmark_config.get("driver_memory", "8g,10g")
    )
    executor_memory = args.executor_memory or str(
        benchmark_config.get("executor_memory", "3g")
    )
    cores = args.cores or str(benchmark_config.get("cores", "3,4"))
    max_partition_bytes = args.max_partition_bytes or str(
        benchmark_config.get("max_partition_bytes", "256MB")
    )
    compression = args.compression or str(benchmark_config.get("compression", "none,snappy"))
    processing_mode = args.processing_mode or str(
        benchmark_config.get("processing_mode", "full_batch")
    )
    file_limit = args.file_limit or benchmark_config.get("file_limit", 2)
    output_path = args.output or str(
        benchmark_config.get("output", "data/benchmark/spark_config_benchmark.csv")
    )

    # Build parameter grid and generate trials
    grid = {
        "shuffle_partitions": parse_int_list(shuffle_partitions),
        "coalesce_n": parse_int_list(coalesce),
        "driver_memory": parse_str_list(driver_memory),
        "executor_memory": parse_str_list(executor_memory),
        "cores": parse_int_list(cores),
        "max_partition_bytes": parse_str_list(max_partition_bytes),
        "compression": parse_str_list(compression),
    }
    trials = build_trials(grid)
    logger.info("Starting benchmark with %d trials", len(trials))

    # Execute benchmark
    results = run_benchmark(trials, config, processing_mode, file_limit)

    # Write results
    final_output = get_output_path(output_path, processing_mode)
    write_results(results, final_output)
    print_best_trial(results)


if __name__ == "__main__":
    main()
