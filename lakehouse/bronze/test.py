from lakehouse.spark_session import get_spark
from lakehouse.bronze.ingest_content_logs import BRONZE_CONTENT_PATH

spark = get_spark("check_bronze_data")

# Đọc bảng Delta Lake vừa tạo
df_bronze = spark.read.format("delta").load(BRONZE_CONTENT_PATH)

# In tổng số dòng và 5 dòng đầu
df_bronze = spark.read.format("delta").load(BRONZE_CONTENT_PATH)
df_bronze.groupBy("event_date").count().orderBy("event_date").show(31)

spark.stop()
