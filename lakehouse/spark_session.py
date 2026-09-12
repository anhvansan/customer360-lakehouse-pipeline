# -*- coding: utf-8 -*-
r"""
Customer360 Lakehouse - Khởi tạo SparkSession có Delta Lake.

Mọi job đều gọi `get_spark(app_name)` để có một SparkSession đã bật sẵn:
  - Delta SQL extension (DeltaSparkSessionExtension)
  - Delta catalog (DeltaCatalog)
  - Tự động tải jar delta-spark qua `configure_spark_with_delta_pip`
    nên không cần khai báo --packages thủ công khi chạy bằng `python file.py`.

Yêu cầu: pip install pyspark delta-spark (xem requirements_lakehouse.txt).
"""

from __future__ import annotations
import os

# --- TRIỆT TIÊU XUNG ĐỘT BẢN SPARK HỆ THỐNG ---
# Xóa SPARK_HOME nếu có để ép PySpark sử dụng đúng bản trong môi trường virtualenv/conda (v3.5.0)
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

from pyspark.sql import SparkSession
from lakehouse.config import RAW_CONTENT_DIR, RAW_SEARCH_DIR

try:
    # delta-spark cung cấp helper gắn đúng version jar tương thích với PySpark.
    from delta import configure_spark_with_delta_pip  # type: ignore
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Thiếu package 'delta-spark'. Cài bằng: pip install delta-spark"
    ) from exc


def get_spark(app_name: str = "Customer360_Lakehouse", driver_memory: str = "4g") -> SparkSession:
    """Tạo (hoặc lấy lại) SparkSession đã cấu hình Delta Lake."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        # --- Bật Delta Lake ---
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Cho phép schema evolution mặc định khi MERGE/append.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Giảm số file nhỏ khi ghi (auto compaction / optimize write của Delta).
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        # Shuffle partitions vừa phải cho dữ liệu cỡ local/demo.
        .config("spark.sql.shuffle.partitions", "8")
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
