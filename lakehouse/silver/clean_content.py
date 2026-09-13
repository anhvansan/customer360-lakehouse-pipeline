# -*- coding: utf-8 -*-
"""
Silver: bronze/content_logs -> silver/content_events.
Chuẩn hóa dữ liệu content_logs từ Bronze, ép kiểu dữ liệu, loại bỏ rác,
và ghi đè (hoặc append) vào tầng Silver dưới dạng Delta Lake.
"""

from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import BRONZE_ROOT, SILVER_ROOT

BRONZE_CONTENT_PATH = f"{BRONZE_ROOT}/content_logs"
SILVER_CONTENT_PATH = f"{SILVER_ROOT}/content_events"


def clean_content_logs(df: DataFrame) -> DataFrame:
    """
    Chuẩn hóa và làm sạch log xem nội dung:
    - Map AppName -> content_type (Tiếng Việt có dấu theo nghiệp vụ)
    - Ép kiểu dữ liệu (explicit casting) và chuẩn hóa tên cột về snake_case
    - Lọc bỏ bản ghi lỗi (Contract null/'0', duration <= 0, content_type không hợp lệ)
    """
    df_clean = (
        df.withColumn(
            "content_type",
            F.when(F.col("AppName") == "CHANNEL", "Truyền Hình")
            .when(F.col("AppName") == "RELAX", "Giải Trí")
            .when(F.col("AppName") == "CHILD", "Thiếu Nhi")
            .when((F.col("AppName") == "FIMS") | (F.col("AppName") == "VOD"), "Phim Truyện")
            .when((F.col("AppName") == "KPLUS") | (F.col("AppName") == "SPORT"), "Thể Thao")
            .otherwise(None)
        )
        .select(
            F.col("Contract").cast("string").alias("contract"),
            "content_type",
            F.col("TotalDuration").cast("double").alias("total_duration"),
            "event_date"
        )
        .filter(F.col("contract").isNotNull() & (F.col("contract") != "0"))
        .filter(F.col("content_type").isNotNull())
        .filter(F.col("total_duration").isNotNull() & (F.col("total_duration") > 0))
    )

    return df_clean


def run(spark: SparkSession, mode: str = "overwrite") -> None:
    """Đọc dữ liệu từ Bronze, thực hiện cleaning và ghi ra tầng Silver."""
    print(f"[silver] Đang đọc dữ liệu từ: {BRONZE_CONTENT_PATH}")
    bronze_df = spark.read.format("delta").load(BRONZE_CONTENT_PATH)

    events_df = clean_content_logs(bronze_df)

    (
        events_df.write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .save(SILVER_CONTENT_PATH)
    )

    print(
        f"[silver] content_events: Đã ghi thành công {events_df.count()} dòng -> {SILVER_CONTENT_PATH}")


if __name__ == "__main__":
    from lakehouse.spark_session import get_spark

    spark = get_spark("clean_content")
    try:
        run(spark)
    finally:
        spark.stop()
