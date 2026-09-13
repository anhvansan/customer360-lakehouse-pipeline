# -*- coding: utf-8 -*-
r"""
Customer360 Lakehouse - Hàm dùng chung cho mọi tầng (Bronze, Silver, Gold).

Bao gồm:
  - clean_keyword():    Logic làm sạch keyword (dùng chung cho Batch & Streaming).
  - read_delta():       Đọc bảng Delta Lake.
  - overwrite_delta(): Ghi đè toàn bộ bảng Delta (full refresh).
  - append_delta():    Thêm dữ liệu vào bảng Delta (append-only).
  - upsert_delta():     MERGE (upsert) DataFrame vào Delta theo Business Keys (Incremental).
  - export_single_csv(): Xuất 1 bảng Delta ra 1 file CSV phẳng đơn lẻ.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import List, Union

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from . import config

PathLike = Union[str, Path]


# =============================================================================
# LÀM SẠCH KEYWORD (dùng chung batch + streaming)
# =============================================================================
def clean_keyword(df: DataFrame, month_label_col_or_value: Union[str, Column]) -> DataFrame:
    """Làm sạch search keyword."""
    if isinstance(month_label_col_or_value, str):
        month_label = F.lit(month_label_col_or_value)
    else:
        month_label = month_label_col_or_value

    cleaned = (
        df
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("keyword").isNotNull())
        .withColumn("user_id", F.trim(F.col("user_id").cast("string")))
        .withColumn("keyword_raw", F.col("keyword").cast("string"))
        .withColumn("keyword", F.lower(F.trim(F.col("keyword_raw"))))
        .withColumn("keyword", F.regexp_replace(F.col("keyword"), config.URL_REGEX, " "))
        .withColumn("keyword", F.regexp_replace(F.col("keyword"), r"[^0-9a-zA-ZÀ-ỹà-ỹ\s]+", " "))
        .withColumn("keyword", F.regexp_replace(F.col("keyword"), r"\s+", " "))
        .withColumn("keyword", F.trim(F.col("keyword")))
        .filter(F.col("user_id") != "")
        .filter(F.col("keyword") != "")
        .filter(F.length(F.col("keyword")) >= 2)
        .filter(~F.col("keyword").rlike(config.ONLY_DIGITS_REGEX))
        .filter(F.col("keyword").rlike(config.HAS_ALNUM_REGEX))
        .filter(~F.col("keyword").isin(*sorted(config.BAD_EXACT_KEYWORDS)))
        .withColumn("month_label", month_label)
        .select("user_id", "keyword", "keyword_raw", "month_label")
    )
    return cleaned


# =============================================================================
# THAO TÁC DELTA LAKE I/O
# =============================================================================
def read_delta(spark: SparkSession, table_path: PathLike) -> DataFrame:
    """Đọc một bảng Delta Lake."""
    return spark.read.format("delta").load(str(table_path))


def overwrite_delta(df: DataFrame, table_path: PathLike, partition_by: List[str] | None = None) -> None:
    """Ghi đè toàn bộ một bảng Delta (full refresh)."""
    writer = df.write.format("delta").mode(
        "overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(table_path))


def append_delta(df: DataFrame, table_path: PathLike, partition_by: List[str] | None = None) -> None:
    """Append (chỉ thêm) vào một bảng Delta - dùng cho bronze append-only."""
    writer = df.write.format("delta").mode(
        "append").option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(table_path))


def upsert_delta(
    spark: SparkSession,
    df: DataFrame,
    table_path: PathLike,
    keys: List[str],
    partition_by: List[str] | None = None,
) -> None:
    """MERGE (upsert) df vào bảng Delta theo danh sách khóa `keys`."""
    table_path_str = str(table_path)

    if not DeltaTable.isDeltaTable(spark, table_path_str):
        overwrite_delta(df, table_path, partition_by)
        return

    delta_table = DeltaTable.forPath(spark, table_path_str)
    condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    (
        delta_table.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# =============================================================================
# EXPORT CSV PHẲNG (Dùng cho BI / Báo cáo)
# =============================================================================
def export_single_csv(df: DataFrame, output_file: PathLike) -> None:
    """Xuất DataFrame ra đúng 1 file .csv phẳng (gộp các part-file Spark)."""
    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = out_path.parent / f"tmp_{out_path.stem}"

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if out_path.exists():
        out_path.unlink()

    df.coalesce(1).write.mode("overwrite").option(
        "header", "true").csv(str(temp_dir))

    part_file = None
    for name in os.listdir(temp_dir):
        if name.startswith("part-") and name.endswith(".csv"):
            part_file = temp_dir / name
            break
    if part_file is None:
        raise FileNotFoundError(f"Không tìm thấy part-file trong {temp_dir}")

    shutil.move(str(part_file), str(out_path))
    shutil.rmtree(temp_dir)
