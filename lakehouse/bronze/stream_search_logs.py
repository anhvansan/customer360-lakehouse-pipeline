# -*- coding: utf-8 -*-
r"""
BRONZE (STREAMING / near-realtime) - Ingest search logs theo luồng.

Spark Structured Streaming theo dõi STREAM_LANDING_DIR; file parquet mới rơi
vào (dạng subfolder YYYYMMDD/ giống cấu trúc raw batch) sẽ tự động nạp vào
Delta bronze/search_logs mà không cần chạy lại batch.

Chạy:  python -m lakehouse.bronze.stream_search_logs
Dừng:  Ctrl+C (chế độ microbatch).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .. import config
from ..spark_session import get_spark


def infer_search_schema(spark: SparkSession) -> StructType:
    """Suy ra schema parquet từ landing dir; nếu trống, lấy từ raw search dir (đã có sẵn batch)."""
    landing = config.STREAM_LANDING_DIR
    if os.path.exists(landing) and any(
        f.endswith(".parquet") for _, _, files in os.walk(landing) for f in files
    ):
        return spark.read.parquet(landing).schema

    if os.path.exists(config.RAW_SEARCH_DIR):
        for name in sorted(os.listdir(config.RAW_SEARCH_DIR)):
            sample = os.path.join(config.RAW_SEARCH_DIR, name)
            if os.path.isdir(sample):
                return spark.read.parquet(sample).schema

    raise FileNotFoundError(
        "Không suy ra được schema: hãy đảm bảo có dữ liệu parquet ở landing hoặc raw dir."
    )


def build_trigger() -> dict:
    if config.STREAM_MODE == "availableNow":
        return {"availableNow": True}
    return {"processingTime": config.STREAM_TRIGGER_INTERVAL}


def main() -> None:
    spark = get_spark("C360_Bronze_Search_Stream")
    try:
        os.makedirs(config.STREAM_LANDING_DIR, exist_ok=True)
        schema = infer_search_schema(spark)

        # 1. Đọc stream từ landing directory
        raw_stream = (
            spark.readStream
            .schema(schema)
            .option("maxFilesPerTrigger", 5)
            .parquet(config.STREAM_LANDING_DIR)
        )

        # 2. Thêm metadata và trích xuất event_date
        stream_df = (
            raw_stream
            .withColumn("source_file", F.input_file_name())
            .withColumn(
                "event_date",
                F.to_date(F.regexp_extract(
                    F.col("source_file"), r"(\d{8})", 1), "yyyyMMdd"),
            )
            .withColumn("ingest_ts", F.current_timestamp())
        )

        # 3. Ghi stream vào bảng Delta Bronze
        query = (
            stream_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", config.checkpoint_path("bronze_search_stream"))
            .option("mergeSchema", "true")
            .partitionBy("event_date")
            .trigger(**build_trigger())
            .start(config.BRONZE_SEARCH)
        )

        print(f"[bronze.stream] Đang theo dõi: {config.STREAM_LANDING_DIR}")
        print(f"[bronze.stream] Ghi vào      : {config.BRONZE_SEARCH}")
        print(
            f"[bronze.stream] Trigger      : {config.STREAM_MODE} ({config.STREAM_TRIGGER_INTERVAL})")
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
