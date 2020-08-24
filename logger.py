import logging
import logging.handlers


def get_log_level(num):
    if num == 0:
        return logging.DEBUG
    elif num == 1:
        return logging.INFO
    elif num == 2:
        return logging.WARN
    elif num == 3:
        return logging.ERROR
    elif num == 4:
        return logging.CRITICAL


# Set up a specific logger with our desired output level
logger = logging.getLogger('MY_SCRAPER')
logger.setLevel(get_log_level(0))

formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
streamhandler = logging.StreamHandler()
streamhandler.setFormatter(formatter)
logger.addHandler(streamhandler)
