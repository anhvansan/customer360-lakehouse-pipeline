# -*- coding: utf-8 -*-
r"""
GOLD - Mart customer_content_profile (Delta, upsert).

Từ Delta `silver/content_events` (cột: contract, content_type, total_duration, event_date):
  1) Aggregate tổng thời lượng xem theo 5 nhóm nội dung cho toàn kỳ.
  2) Tính most_watch_type dựa trên tổng duration lớn nhất toàn kỳ.
  3) Ghép taste_profile từ các nhóm nội dung có thời lượng > 0.
  4) Tính active_days và activity_level (High nếu active_days > 4).
  5) MERGE (upsert) vào Delta `gold/customer_content_profile` theo contract.

Chạy:  python -m lakehouse.gold.customer_content_profile
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .. import config
from ..common import read_delta, upsert_delta
from ..spark_session import get_spark

# Map giá trị content_type (Tiếng Việt có dấu, đúng như clean_content.py xuất ra) -> tên cột Gold
_TYPE_TO_COL = {
    "Giải Trí": "total_giai_tri",
    "Phim Truyện": "total_phim_truyen",
    "Thể Thao": "total_the_thao",
    "Thiếu Nhi": "total_thieu_nhi",
    "Truyền Hình": "total_truyen_hinh",
}


def build_content_profile(events_df: DataFrame) -> DataFrame:
    # 1. Lọc theo khoảng thời gian kỳ báo cáo
    filtered_df = events_df.filter(
        (F.col("event_date") >= config.PERIOD_START) & (
            F.col("event_date") <= config.PERIOD_END)
    )

    # 2. Gom nhóm toàn kỳ theo contract — dùng đúng tên cột snake_case của Silver:
    #    contract, content_type, total_duration
    contract_summary = (
        filtered_df.groupBy("contract")
        .agg(
            F.count_distinct("event_date").alias("active_days"),
            *[
                F.sum(F.when(F.col("content_type") == t, F.col(
                    "total_duration")).otherwise(0)).alias(col_name)
                for t, col_name in _TYPE_TO_COL.items()
            ],
        )
    )

    # 3. Tính toán Activity Level, Most Watched Type & Taste Profile
    profile_df = (
        contract_summary
        .withColumn(
            "activity_level",
            F.when(F.col("active_days") > config.HIGH_ACTIVITY_DAY_THRESHOLD, F.lit(
                "High")).otherwise(F.lit("Low")),
        )
        .withColumn(
            "max_duration",
            F.greatest(*[F.col(c) for c in _TYPE_TO_COL.values()]),
        )
        .withColumn(
            "most_watch_type",
            F.when(F.col("max_duration") == 0, F.lit("None"))
             .when(F.col("max_duration") == F.col("total_truyen_hinh"), F.lit("Truyền Hình"))
             .when(F.col("max_duration") == F.col("total_phim_truyen"), F.lit("Phim Truyện"))
             .when(F.col("max_duration") == F.col("total_the_thao"), F.lit("Thể Thao"))
             .when(F.col("max_duration") == F.col("total_thieu_nhi"), F.lit("Thiếu Nhi"))
             .otherwise(F.lit("Giải Trí")),
        )
        .withColumn(
            "taste_profile",
            F.concat_ws(
                "-",
                *[
                    F.when(F.col(col_name) > 0, F.lit(t))
                    for t, col_name in _TYPE_TO_COL.items()
                ],
            ),
        )
        .drop("max_duration")
    )

    return profile_df


def main() -> None:
    spark = get_spark("C360_Gold_ContentProfile")
    try:
        events_df = read_delta(spark, config.SILVER_CONTENT_EVENTS)
        profile_df = build_content_profile(events_df)

        upsert_delta(
            spark,
            profile_df,
            config.GOLD_CONTENT_PROFILE,
            keys=config.KEY_CONTENT_PROFILE,
        )
        print(
            f"[gold.content_profile] upsert {profile_df.count()} contract -> {config.GOLD_CONTENT_PROFILE}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
