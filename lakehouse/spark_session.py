# -*- coding: utf-8 -*-
r"""
Customer360 Lakehouse - Khởi tạo SparkSession có Delta Lake.
"""

from __future__ import annotations
from lakehouse.config import RAW_CONTENT_DIR, RAW_SEARCH_DIR
from pyspark.sql import SparkSession

import os
import sys

# --- BẮT BUỘC set TRƯỚC khi import pyspark ---
# Ép driver VÀ worker dùng đúng python trong venv hiện tại.
# Thiếu bước này, PySpark trên Windows có thể spawn worker bằng python khác
# (không có pyspark/delta cài, hoặc khác version) -> worker crash ngay khi
# chạy UDF (không lỗi khi chỉ dùng Spark native function).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]


try:
    from delta import configure_spark_with_delta_pip
except ImportError as exc:
    raise ImportError(
        "Thiếu package 'delta-spark'. Cài bằng: pip install delta-spark"
    ) from exc


def get_spark(app_name: str = "Customer360_Lakehouse", driver_memory: str = "4g") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        # Ép executor cũng nhận đúng biến môi trường Python (đề phòng trường hợp
        # local mode không tự propagate từ os.environ của driver)
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    print("content exists:", os.path.exists(RAW_CONTENT_DIR))
    print("search exists:", os.path.exists(RAW_SEARCH_DIR))

    spark = get_spark("test_session")
    print("Spark Version đang sử dụng:", spark.version)
    spark.sql("SELECT 1 AS test").show()
    spark.stop()
