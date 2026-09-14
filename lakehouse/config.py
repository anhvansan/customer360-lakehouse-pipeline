import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Raw data nằm ngoài repo, trỏ thẳng — dùng raw string (r"...") vì Windows path có "\"
RAW_CONTENT_DIR = r"D:\444 ETL Big Data 2026\00 Dataset\log_content"
RAW_SEARCH_DIR = r"D:\444 ETL Big Data 2026\00 Dataset\log_search"

# Bronze/Silver/Gold thì vẫn nên nằm trong project (dễ quản lý, tự sinh ra khi chạy)
BRONZE_ROOT = os.path.join(PROJECT_ROOT, "data", "bronze")
SILVER_ROOT = os.path.join(PROJECT_ROOT, "data", "silver")
GOLD_ROOT = os.path.join(PROJECT_ROOT, "data", "gold")

REFERENCE_ROOT = os.path.join(PROJECT_ROOT, "reference")
BRIDGE_TABLE_PATH = os.path.join(REFERENCE_ROOT, "customer_key_bridge.csv")
MAPPING_CSV_PATH = os.path.join(REFERENCE_ROOT, "keyword_category_mapping.csv")

# =============================================================================
# DELTA TABLE PATHS
# =============================================================================
BRONZE_CONTENT = os.path.join(BRONZE_ROOT, "content_logs")
BRONZE_SEARCH = os.path.join(BRONZE_ROOT, "search_logs")

SILVER_CONTENT_EVENTS = os.path.join(SILVER_ROOT, "content_events")
SILVER_SEARCH_CLEAN = os.path.join(SILVER_ROOT, "search_clean")

GOLD_CONTENT_PROFILE = os.path.join(GOLD_ROOT, "customer_content_profile")
GOLD_SEARCH_TREND = os.path.join(GOLD_ROOT, "customer_search_trend")
GOLD_CUSTOMER360 = os.path.join(GOLD_ROOT, "customer360_profile")

EXPORT_DIR = os.path.join(GOLD_ROOT, "_exports")

# =============================================================================
# BUSINESS KEYS dùng cho MERGE/upsert
# =============================================================================
KEY_CONTENT_PROFILE = ["contract"]
KEY_SEARCH_TREND = ["user_id"]
KEY_CUSTOMER360 = ["user_id"]

# =============================================================================
# THAM SỐ NGHIỆP VỤ
# =============================================================================
HIGH_ACTIVITY_DAY_THRESHOLD = 4  # active_days > 4 -> High

PERIOD_START = "2022-04-01"
PERIOD_END = "2022-04-30"

MONTH_PREFIXES = {
    "t6": "202206",
    "t7": "202207",
}

MIN_COUNT_PER_MONTH = 2  # dùng ở gold/customer_search_trend: chỉ giữ keyword tìm >=2 lần

# Regex làm sạch keyword — dùng chung cho clean_search.py và common.clean_keyword()
URL_REGEX = r"(http|https|www\.)\S*"
ONLY_DIGITS_REGEX = r"^[0-9]+$"
HAS_ALNUM_REGEX = r".*[a-zA-ZÀ-ỹà-ỹ0-9].*"
BAD_EXACT_KEYWORDS = ["null", "none", "na",
                      "n/a", "undefined", "unk", "unknown", ""]

STREAM_LANDING_DIR = os.path.join(
    PROJECT_ROOT, "data", "stream_landing", "log_search")
CHECKPOINT_ROOT = os.path.join(PROJECT_ROOT, "data", "_checkpoints")


def checkpoint_path(name: str) -> str:
    return os.path.join(CHECKPOINT_ROOT, name)


# "microbatch" (chạy liên tục) hoặc "availableNow" (xử lý hết rồi dừng)
STREAM_MODE = "availableNow"
STREAM_TRIGGER_INTERVAL = "30 seconds"
