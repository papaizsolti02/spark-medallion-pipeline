import argparse
import csv
import json
from itertools import product
from pathlib import Path
from time import perf_counter
from typing import Dict, Iterable, List

from src.core.config_loader import load_config
from src.core.spark import create_spark
from src.jobs.raw_to_bronze import run as raw_to_bronze
from src.utils.logger import get_logger


logger = get_logger("benchmark")


def parse_int_list(value: str) -> List[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def parse_str_list(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def build_trials(grid: Dict[str, List]) -> Iterable[Dict[str, object]]:
    keys = list(grid.keys())
    for combo in product(*(grid[key] for key in keys)):
        yield dict(zip(keys, combo))


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def main() -> None:
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
        help="Bronze ingestion mode. full_batch reads all selected files in one Spark job.",
    )
    parser.add_argument(
        "--file-limit",
        type=int,
        default=None,
        help="Optional cap on number of files. Omit to use all files.",
    )
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    benchmark_config = config.get("benchmark", {})

    shuffle_partitions = args.shuffle_partitions or str(
        benchmark_config.get("shuffle_partitions", "12,24")
    )
    coalesce = args.coalesce or str(benchmark_config.get("coalesce", "4,8"))
    driver_memory = args.driver_memory or str(benchmark_config.get("driver_memory", "8g,10g"))
    executor_memory = args.executor_memory or str(benchmark_config.get("executor_memory", "3g"))
    cores = args.cores or str(benchmark_config.get("cores", "3,4"))
    max_partition_bytes = args.max_partition_bytes or str(
        benchmark_config.get("max_partition_bytes", "256MB")
    )
    compression = args.compression or str(benchmark_config.get("compression", "none,snappy"))
    processing_mode = args.processing_mode or str(
        benchmark_config.get("processing_mode", "full_batch")
    )
    file_limit = args.file_limit
    if file_limit is None and benchmark_config.get("file_limit") is not None:
        file_limit = int(benchmark_config.get("file_limit"))
    output_path_raw = args.output or str(
        benchmark_config.get("output", "data/benchmark/spark_config_benchmark.csv")
    )

    grid = {
        "shuffle_partitions": parse_int_list(shuffle_partitions),
        "coalesce_n": parse_int_list(coalesce),
        "driver_memory": parse_str_list(driver_memory),
        "executor_memory": parse_str_list(executor_memory),
        "cores": parse_int_list(cores),
        "max_partition_bytes": parse_str_list(max_partition_bytes),
        "compression": parse_str_list(compression),
    }

    trials = list(build_trials(grid))
    logger.info("Starting benchmark with %s trials", len(trials))

    results = []
    for idx, trial in enumerate(trials, 1):
        logger.info("Trial %s/%s: %s", idx, len(trials), json.dumps(trial))
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

            raw_result = raw_to_bronze(
                spark,
                config,
                processing_mode=processing_mode,
                coalesce_n=int(trial["coalesce_n"]),
                compression=str(trial["compression"]),
                file_limit=file_limit,
                count_rows=False,
                logger=logger,
            )
            files_processed = raw_result.get("files_processed", 0)
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            files_processed = 0
            error = str(exc)
            logger.exception("Trial failed")
        finally:
            if spark is not None:
                try:
                    spark.stop()
                    logger.info("Spark session stopped successfully")
                except Exception as stop_exc:  # noqa: BLE001
                    logger.warning("Failed to stop Spark session cleanly: %s", stop_exc)
                    # JVM may have crashed, force cleanup
                    try:
                        import os
                        import signal
                        # Give it a moment then continue
                        import time
                        time.sleep(2)
                    except Exception:  # noqa: BLE001
                        pass

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
        logger.info("Trial %s finished in %ss (%s)", idx, elapsed, status)

    # Create separate CSV files for different processing modes
    output_path = Path(output_path_raw)
    stem = output_path.stem  # e.g., 'spark_config_benchmark'
    suffix = output_path.suffix  # e.g., '.csv'
    output_path = output_path.parent / f"{stem}_{processing_mode}{suffix}"
    
    ensure_parent(output_path)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)

    best = sorted(
        [r for r in results if r["status"] == "ok"],
        key=lambda item: item["seconds"],
    )

    if best:
        logger.info("Best trial: %s", json.dumps(best[0]))
    logger.info("Benchmark results written to %s", output_path)


if __name__ == "__main__":
    main()
