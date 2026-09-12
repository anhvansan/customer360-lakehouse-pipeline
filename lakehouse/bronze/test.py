from lakehouse.spark_session import get_spark
from lakehouse.bronze.ingest_content_logs import read_content_logs
from lakehouse.config import RAW_CONTENT_DIR

spark = get_spark("test_content")

# đổi tên file thật của bạn vào đây, 1 ngày duy nhất
one_file = RAW_CONTENT_DIR + r"\20220401.json"

df = read_content_logs(spark, one_file)
df.printSchema()
df.show(5, truncate=False)
