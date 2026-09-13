# -*- coding: utf-8 -*-
"""
Silver: bronze/search_logs -> silver/search_clean.
Chuẩn hóa dữ liệu tìm kiếm: gỡ bỏ ký tự đặc biệt, chuẩn hóa tiếng Việt, 
loại bỏ từ khóa rác/null, chuẩn hóa tên cột về dạng snake_case.
"""

from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F

from lakehouse.config import BRONZE_ROOT, SILVER_ROOT

BRONZE_SEARCH_PATH = f"{BRONZE_ROOT}/search_logs"
SILVER_SEARCH_PATH = f"{SILVER_ROOT}/search_clean"


def clean_keyword_expr(col: Column) -> Column:
    """Chuẩn hóa keyword: chuyển về chữ thường, gỡ URL, loại ký tự đặc biệt (giữ tiếng Việt), nén khoảng trắng."""
    c = F.lower(F.trim(col))
    c = F.regexp_replace(c, r"https?://\S+", "")
    c = F.regexp_replace(
        c,
        r"[^a-z0-9\sàáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ]",
        " "
    )
    c = F.regexp_replace(c, r"\s+", " ")
    return F.trim(c)


def clean_search_logs(df: DataFrame) -> DataFrame:
    """
    Làm sạch và chuẩn hóa log tìm kiếm:
    - Loại bỏ bản ghi thiếu user_id hoặc keyword
    - Làm sạch chuỗi keyword (keyword_clean) và lọc bỏ rác (rỗng, quá ngắn, toàn số, từ vô nghĩa)
    - Chuẩn hóa platform và network_type (snake_case, fill unknown)
    """
    # 1. Bắt buộc: Loại bỏ dòng null user_id hoặc keyword
    df_filtered = df.filter(F.col("user_id").isNotNull()
                            & F.col("keyword").isNotNull())

    # 2. Tạo cột keyword_clean
    df_clean = df_filtered.withColumn(
        "keyword_clean", clean_keyword_expr(F.col("keyword")))

    # 3. Loại bỏ từ khóa rác sau khi làm sạch
    df_clean = df_clean.filter(
        (F.length("keyword_clean") >= 2)
        & (~F.col("keyword_clean").rlike(r"^\d+$"))
        & (~F.col("keyword_clean").isin("null", "none", "undefined", ""))
    )

    # 4. Chuẩn hóa tên cột & kiểu dữ liệu sang snake_case
    df_result = df_clean.select(
        F.col("user_id").cast("string").alias("user_id"),
        F.col("keyword_clean").alias("keyword"),
        F.coalesce(F.lower(F.trim(F.col("platform"))),
                   F.lit("unknown")).alias("platform"),
        F.coalesce(F.lower(F.trim(F.col("networkType"))),
                   F.lit("unknown")).alias("network_type"),
        "event_date"
    )

    return df_result


def run(spark: SparkSession, mode: str = "overwrite") -> None:
    """Đọc từ Bronze search_logs, làm sạch và ghi vào Silver search_clean."""
    print(f"[silver] Đang đọc dữ liệu từ: {BRONZE_SEARCH_PATH}")
    bronze_df = spark.read.format("delta").load(BRONZE_SEARCH_PATH)

    search_clean_df = clean_search_logs(bronze_df)

    (
        search_clean_df.write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .save(SILVER_SEARCH_PATH)
    )

    print(
        f"[silver] search_clean: Đã ghi thành công {search_clean_df.count()} dòng -> {SILVER_SEARCH_PATH}")


if __name__ == "__main__":
    from lakehouse.spark_session import get_spark

    spark = get_spark("clean_search")
    try:
        run(spark)
    finally:
        spark.stop()
