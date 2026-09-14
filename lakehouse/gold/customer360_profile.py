# -*- coding: utf-8 -*-
r"""
GOLD - Mart customer360_profile (Delta, upsert) - bảng tổng hợp cuối cùng.

Join:
    gold/customer_search_trend (user_id)
      + reference/customer_key_bridge.csv (user_id <-> contract)
      + gold/customer_content_profile (contract)
-> MERGE (upsert) vào Delta `gold/customer360_profile` theo user_id.

Đồng thời export 1 file CSV phẳng (cho BI cũ) trong gold/_exports.

Chạy:  python -m lakehouse.gold.customer360_profile
"""

from __future__ import annotations

import os
from pyspark.sql import functions as F

from .. import config
from ..common import export_single_csv, read_delta, upsert_delta
from ..spark_session import get_spark

FINAL_COLS = [
    "user_id", "contract",
    "keyword_t6", "search_count_t6", "category_t6",
    "keyword_t7", "search_count_t7", "category_t7",
    "keyword_changed_flag", "category_shift_flag", "category_transition",
    "most_watch_type", "taste_profile", "activity_level", "active_days",
    "total_giai_tri", "total_phim_truyen", "total_the_thao",
    "total_thieu_nhi", "total_truyen_hinh",
    "updated_at"
]


def main() -> None:
    bridge_path = str(config.BRIDGE_TABLE_PATH)
    if not os.path.exists(bridge_path):
        raise FileNotFoundError(
            f"Không tìm thấy bridge file: {bridge_path}\n"
            "Cần file mapping user_id <-> contract trước khi build Customer360."
        )

    spark = get_spark("C360_Gold_Customer360")
    try:
        search_df = read_delta(spark, config.GOLD_SEARCH_TREND)
        content_df = read_delta(spark, config.GOLD_CONTENT_PROFILE)

        # Đọc schema file bridge để handle tên cột Contract/contract linh hoạt
        raw_bridge = spark.read.option("header", "true").csv(bridge_path)
        contract_col_name = "Contract" if "Contract" in raw_bridge.columns else "contract"

        bridge_df = (
            raw_bridge
            .select(
                F.col("user_id").cast("string").alias("user_id"),
                F.col(contract_col_name).cast("string").alias("contract"),
            )
            # Đảm bảo 1 user_id duy nhất chỉ map với 1 contract
            .dropDuplicates(["user_id"])
        )

        final_df = (
            search_df.alias("s")
            .join(bridge_df.alias("b"), on="user_id", how="left")
            .join(content_df.alias("c"), on="contract", how="left")
            .withColumn("updated_at", F.current_timestamp())
        )

        # Chỉ chọn các cột có tồn tại thực tế để tránh lỗi AnalysisException
        output_cols = [c for c in FINAL_COLS if c in final_df.columns]
        output_df = final_df.select(*output_cols)

        # 1. Upsert vào kho Delta Lake Gold
        upsert_delta(spark, output_df, config.GOLD_CUSTOMER360,
                     keys=config.KEY_CUSTOMER360)

        # 2. Export CSV phẳng cho bộ phận BI
        export_file = os.path.join(
            config.EXPORT_DIR, "mart_customer360_profile.csv")
        export_single_csv(read_delta(
            spark, config.GOLD_CUSTOMER360), export_file)

        print(
            f"[gold.customer360] Upsert thành công {output_df.count()} user -> {config.GOLD_CUSTOMER360}")
        print(f"[gold.customer360] Export CSV -> {export_file}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
