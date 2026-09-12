# -*- coding: utf-8 -*-
"""
Bronze: log_content (json theo ngày) -> bronze Delta.
Append-only, giữ nguyên field gốc, chỉ thêm metadata (event_date, source_file, ingest_ts).
"""

from __future__ import annotations
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import RAW_CONTENT_DIR, BRONZE_ROOT

BRONZE_CONTENT_PATH = f"{BRONZE_ROOT}/content_logs"


def read_content_logs(spark: SparkSession, path: str) -> DataFrame:
    """Đọc 1 hoặc nhiều file json (NDJSON, dạng export Elasticsearch)."""
    df_raw = spark.read.json(path)

    # Field thật nằm trong "_source" -> flatten ra
    df = df_raw.select("_source.*")

    # event_date KHÔNG có sẵn trong JSON -> lấy từ tên file, vd .../20220401.json
    df = df.withColumn("source_file", F.input_file_name())
    df = df.withColumn(
        "event_date",
        F.to_date(
            F.regexp_extract(F.col("source_file"), r"(\d{8})\.json", 1),
            "yyyyMMdd"
        ),
    )
    df = df.withColumn("ingest_ts", F.current_timestamp())
    return df


def ingest_content_logs(spark: SparkSession, path: str | None = None, mode: str = "append") -> None:
    """Ingest toàn bộ file JSON vào tầng Bronze Delta Lake."""
    read_path = path or f"{RAW_CONTENT_DIR}/*.json"
    df = read_content_logs(spark, read_path)

    (
        df.write
        .format("delta")
        .mode(mode)
        # Phân vùng theo ngày giúp truy vấn tầng Silver nhanh hơn
        .partitionBy("event_date")
        .option("mergeSchema", "true")
        .save(BRONZE_CONTENT_PATH)
    )
    print(f"[bronze] content_logs: {df.count()} rows -> {BRONZE_CONTENT_PATH}")


if __name__ == "__main__":
    from lakehouse.spark_session import get_spark

    spark = get_spark("ingest_content_logs")
    ingest_content_logs(spark)
    spark.stop()
