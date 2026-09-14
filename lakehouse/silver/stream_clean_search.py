# -*- coding: utf-8 -*-
r"""
SILVER (STREAMING / near-realtime) - Làm sạch search theo luồng từ bronze.

Đọc Delta bronze/search_logs dưới dạng STREAM, tái sử dụng hàm clean_search_logs()
(giống hệt batch) để đảm bảo logic làm sạch không bị lệch giữa 2 luồng.

Chạy:  python -m lakehouse.silver.stream_clean_search
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from .. import config
from ..common import append_delta
from ..silver.clean_search import clean_search_logs
from ..spark_session import get_spark


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    clean_df = clean_search_logs(batch_df)
    append_delta(clean_df, config.SILVER_SEARCH_CLEAN,
                 partition_by=["event_date"])
    print(f"[silver.stream] batch {batch_id}: +{clean_df.count()} dòng sạch")


def build_trigger() -> dict:
    if config.STREAM_MODE == "availableNow":
        return {"availableNow": True}
    return {"processingTime": config.STREAM_TRIGGER_INTERVAL}


def main() -> None:
    spark = get_spark("C360_Silver_CleanSearch_Stream")
    try:
        bronze_stream = spark.readStream.format(
            "delta").load(config.BRONZE_SEARCH)

        query = (
            bronze_stream.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", config.checkpoint_path("silver_search_stream"))
            .trigger(**build_trigger())
            .start()
        )

        print(f"[silver.stream] bronze --> silver ({config.STREAM_MODE})")
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
