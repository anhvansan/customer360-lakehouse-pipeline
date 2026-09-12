# -*- coding: utf-8 -*-
"""
Bronze: log_search (parquet theo ngày) -> bronze Delta.
Append-only, giữ nguyên schema gốc, bổ sung metadata và phân vùng event_date.
"""

from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import RAW_SEARCH_DIR, BRONZE_ROOT

BRONZE_SEARCH_PATH = f"{BRONZE_ROOT}/search_logs"


def read_search_logs(spark: SparkSession, path: str) -> DataFrame:
    """Đọc dữ liệu parquet search logs đệ quy từ tất cả thư mục ngày."""
    # Thêm option recursiveFileLookup để Spark tự quét vào toàn bộ thư mục con (20220601, 20220602,...)
    df = spark.read.option("recursiveFileLookup", "true").parquet(path)

    # Lấy đường dẫn file nguồn
    df = df.withColumn("source_file", F.input_file_name())

    # Trích xuất event_date từ đường dẫn thư mục (VD: .../log_search/20220601/part-... -> 2022-06-01)
    df = df.withColumn(
        "event_date",
        F.to_date(
            F.regexp_extract(F.col("source_file"), r"(\d{8})", 1),
            "yyyyMMdd"
        )
    )

    df = df.withColumn("ingest_ts", F.current_timestamp())
    return df


def ingest_search_logs(spark: SparkSession, path: str | None = None, mode: str = "append") -> None:
    read_path = path or RAW_SEARCH_DIR
    df = read_search_logs(spark, read_path)

    (
        df.write
        .format("delta")
        .mode(mode)
        .partitionBy("event_date")
        .option("mergeSchema", "true")
        .save(BRONZE_SEARCH_PATH)
    )
    print(f"[bronze] search_logs: {df.count()} rows -> {BRONZE_SEARCH_PATH}")


if __name__ == "__main__":
    from lakehouse.spark_session import get_spark

    spark = get_spark("ingest_search_logs")
    ingest_search_logs(spark)
    spark.stop()
