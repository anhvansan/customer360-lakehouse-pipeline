# -*- coding: utf-8 -*-
r"""
Customer360 Lakehouse - Orchestrator chạy toàn bộ pipeline BATCH theo thứ tự Medallion.

Luồng chạy:
    BRONZE: ingest_search_logs, ingest_content_logs
    SILVER: clean_search, clean_content
    GOLD  : customer_search_trend, customer_content_profile, customer360_profile

Chạy:
    python -m lakehouse.run_pipeline
"""

import argparse
import subprocess
import sys
import time

# Danh sách các module theo đúng thứ tự phụ thuộc dữ liệu
PIPELINE_STEPS = [
    # 1. Tầng Bronze (Ingestion)
    "lakehouse.bronze.ingest_search_logs",
    "lakehouse.bronze.ingest_content_logs",

    # 2. Tầng Silver (Cleansing & Transformation)
    "lakehouse.silver.clean_search",
    "lakehouse.silver.clean_content",

    # 3. Tầng Gold (Data Marts & Master Profile)
    "lakehouse.gold.customer_search_trend",
    "lakehouse.gold.customer_content_profile",
    "lakehouse.gold.customer360_profile",
]


def run_step(module_name: str) -> None:
    print(f"RUNNING STEP: {module_name}")

    start_time = time.time()
    # Gõ lệnh chạy module python dưới dạng subprocess độc lập
    result = subprocess.run([sys.executable, "-m", module_name])

    if result.returncode != 0:
        print(f"\nSTEP FAILED: {module_name}")
        sys.exit(result.returncode)

    elapsed = time.time() - start_time
    print(f"COMPLETED: {module_name} in {elapsed:.2f}s")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Customer360 Lakehouse Orchestrator")
    parser.add_argument(
        "--from-stage",
        choices=["bronze", "silver", "gold"],
        default="bronze",
        help="Chạy pipeline từ tầng mong muốn (default: bronze)"
    )
    args = parser.parse_args()

    print("STARTING CUSTOMER360 LAKEHOUSE PIPELINE...")
    total_start = time.time()

    # Lọc danh sách bước chạy nếu người dùng truyền flag --from-stage
    steps_to_run = PIPELINE_STEPS
    if args.from_stage == "silver":
        steps_to_run = [s for s in PIPELINE_STEPS if "bronze" not in s]
    elif args.from_stage == "gold":
        steps_to_run = [s for s in PIPELINE_STEPS if "gold" in s]

    for step in steps_to_run:
        run_step(step)

    total_elapsed = time.time() - total_start
    print(f"\nALL STEPS COMPLETED SUCCESSFUL IN {total_elapsed:.2f}s!")


if __name__ == "__main__":
    main()
