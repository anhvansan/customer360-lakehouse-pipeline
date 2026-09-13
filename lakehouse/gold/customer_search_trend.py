# -*- coding: utf-8 -*-
r"""
GOLD - Mart customer_search_trend (Delta, upsert).

Từ Delta `silver/search_clean`:
  1) Đếm tần suất keyword theo (user_id, tháng), chỉ giữ keyword tìm >= MIN_COUNT_PER_MONTH lần.
  2) Rank top-1 keyword mỗi user mỗi tháng (window row_number).
  3) Map keyword -> category bằng file tĩnh keyword_category_mapping.csv (broadcast join).
  4) Ghép T6-T7 cạnh nhau (inner join user_id, chỉ giữ user có mặt cả 2 tháng).
  5) Tạo tín hiệu: keyword_changed_flag, category_shift_flag, category_transition.
  6) MERGE (upsert) vào Delta `gold/customer_search_trend` theo user_id.

Chạy:  python -m lakehouse.gold.customer_search_trend
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path

from .. import config
from ..common import read_delta, upsert_delta
from ..spark_session import get_spark
from .keyword_mapping import load_mapping_rules, make_categorize_udf


def add_month_label(df: DataFrame) -> DataFrame:
    ym = F.date_format(F.col("event_date"), "yyyyMM")
    month_col = F.lit(None).cast("string")
    for label, prefix in config.MONTH_PREFIXES.items():
        month_col = F.when(ym == F.lit(prefix), F.lit(label)
                           ).otherwise(month_col)
    return df.withColumn("month_label", month_col).filter(F.col("month_label").isNotNull())


def top_keyword_per_user_month(df: DataFrame) -> DataFrame:
    counted = (
        df.groupBy("user_id", "month_label", "keyword")
        .agg(F.count("*").alias("search_count"))
        .filter(F.col("search_count") >= config.MIN_COUNT_PER_MONTH)
    )
    w = Window.partitionBy("user_id", "month_label").orderBy(
        F.col("search_count").desc())
    top1 = (
        counted.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    return top1


def attach_category(top1: DataFrame, categorize_udf) -> DataFrame:
    return top1.withColumn("category", categorize_udf(F.col("keyword")))


def build_search_trend(spark: SparkSession, search_clean_df: DataFrame) -> DataFrame:
    df = add_month_label(search_clean_df)
    top1 = top_keyword_per_user_month(df)

    rules = load_mapping_rules(Path(config.MAPPING_CSV_PATH))
    categorize_udf = make_categorize_udf(rules)
    top1_cat = attach_category(top1, categorize_udf)

    t6 = top1_cat.filter(F.col("month_label") == "t6").select(
        "user_id",
        F.col("keyword").alias("keyword_t6"),
        F.col("category").alias("category_t6"),
        F.col("search_count").alias("search_count_t6"),
    )
    t7 = top1_cat.filter(F.col("month_label") == "t7").select(
        "user_id",
        F.col("keyword").alias("keyword_t7"),
        F.col("category").alias("category_t7"),
        F.col("search_count").alias("search_count_t7"),
    )

    merged = t6.join(t7, on="user_id", how="inner")

    trend_df = (
        merged
        .withColumn("keyword_changed_flag", F.col("keyword_t6") != F.col("keyword_t7"))
        .withColumn("category_shift_flag", F.col("category_t6") != F.col("category_t7"))
        .withColumn(
            "category_transition",
            F.when(
                F.col("category_shift_flag"),
                F.concat_ws(" -> ", F.col("category_t6"),
                            F.col("category_t7")),
            ).otherwise(F.lit(None)),
        )
    )
    return trend_df


def main() -> None:
    spark = get_spark("C360_Gold_SearchTrend")
    try:
        search_clean_df = read_delta(spark, config.SILVER_SEARCH_CLEAN)
        trend_df = build_search_trend(spark, search_clean_df)

        upsert_delta(spark, trend_df, config.GOLD_SEARCH_TREND,
                     keys=config.KEY_SEARCH_TREND)
        print(
            f"[gold.search_trend] upsert {trend_df.count()} user -> {config.GOLD_SEARCH_TREND}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
