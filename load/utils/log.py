import sys
from loguru import logger

logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-dd HH:mm:ss} [{level}] - {message}")
