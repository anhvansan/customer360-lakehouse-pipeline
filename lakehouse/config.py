import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Raw data nằm ngoài repo, trỏ thẳng — dùng raw string (r"...") vì Windows path có "\"
RAW_CONTENT_DIR = r"D:\444 ETL Big Data 2026\00 Dataset\log_content"
RAW_SEARCH_DIR = r"D:\444 ETL Big Data 2026\00 Dataset\log_search"

# Bronze/Silver/Gold thì vẫn nên nằm trong project (dễ quản lý, tự sinh ra khi chạy)
BRONZE_ROOT = os.path.join(PROJECT_ROOT, "data", "bronze")
SILVER_ROOT = os.path.join(PROJECT_ROOT, "data", "silver")
GOLD_ROOT = os.path.join(PROJECT_ROOT, "data", "gold")

BRIDGE_TABLE_PATH = os.path.join(
    PROJECT_ROOT, "reference", "customer_key_bridge.csv")
