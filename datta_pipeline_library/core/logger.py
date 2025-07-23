"""
Logger Module to set logging capabilities.
"""
import logging
from logging.handlers import RotatingFileHandler
import os

LOG_PREFIX = "" if os.environ.get("pipeline") == "Test" else "/dbfs/mnt/cluster/logs/"
LOG_PATH = LOG_PREFIX + "file.log"

def set_logger():
    """
    :rtype: None
    """
    # TODO Need to use ADLS appender or use HTTP client to direclty write into LAW
    logging.basicConfig(
        handlers=[
            RotatingFileHandler(
                LOG_PATH,
                maxBytes=0,
                backupCount=5,
            )
        ],
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s.%(funcName)s %(message)s'
    )

    logger = logging.getLogger("reference_pipeline")
    logger.setLevel(logging.DEBUG)
