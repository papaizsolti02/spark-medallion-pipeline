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
    parser.add_argument("--shuffle-partitions", default="12,24")
    parser.add_argument("--coalesce", default="4,8")
    parser.add_argument("--driver-memory", default="8g,10g")
    parser.add_argument("--executor-memory", default="3g")
    parser.add_argument("--cores", default="3,4")
    parser.add_argument("--max-partition-bytes", default="256MB")
    parser.add_argument("--compression", default="none,snappy")
    parser.add_argument("--file-limit", type=int, default=2)
    parser.add_argument("--output", default="data/benchmark/spark_config_benchmark.csv")
    args = parser.parse_args()

    config = load_config(args.config)

    grid = {
        "shuffle_partitions": parse_int_list(args.shuffle_partitions),
        "coalesce_n": parse_int_list(args.coalesce),
        "driver_memory": parse_str_list(args.driver_memory),
        "executor_memory": parse_str_list(args.executor_memory),
        "cores": parse_int_list(args.cores),
        "max_partition_bytes": parse_str_list(args.max_partition_bytes),
        "compression": parse_str_list(args.compression),
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
                coalesce_n=int(trial["coalesce_n"]),
                compression=str(trial["compression"]),
                file_limit=args.file_limit,
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
                spark.stop()

        elapsed = round(perf_counter() - trial_start, 2)
        results.append(
            {
                **trial,
                "status": status,
                "seconds": elapsed,
                "files_processed": files_processed,
                "error": error,
            }
        )
        logger.info("Trial %s finished in %ss (%s)", idx, elapsed, status)

    output_path = Path(args.output)
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
