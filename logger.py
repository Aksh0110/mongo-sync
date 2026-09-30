import logging
from logging.handlers import RotatingFileHandler
import os

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "sync.log")

os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("MongoSync")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)

# Rotate logs when file reaches 10MB, keep up to 5 backups
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8"
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Avoid adding duplicate handlers if logger is imported multiple times
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

