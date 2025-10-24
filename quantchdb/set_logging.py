import logging
import coloredlogs
import os

# 默认日志文件
DEFAULT_LOG_FILE = "default_log.log"

#默认格式
DEFAULT_LOG_FORMAT =  "%(asctime)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

def get_logger(name=None, log_file=None, level=logging.DEBUG, terminal_log=False, file_log=True):
    """
    Get a logger which support terminal log and file log with switch.
    
    """
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger  

    logger.setLevel(level)

    formatter = logging.Formatter(DEFAULT_LOG_FORMAT)

    colored_formatter = coloredlogs.ColoredFormatter(
        fmt=DEFAULT_LOG_FORMAT,
        level_styles={
            'critical': {'color': 'red', 'bold': True},
            'error': {'color': 'blue'},
            'warning': {'color': 'yellow'},
            'info': {'color': 'green'},
            'debug': {'color': 'blue'},
        },
        field_styles={
            'asctime': {'color': 'cyan'},
            'levelname': {'color': 'cyan'},
        }
    )

    if terminal_log:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(colored_formatter)
        stream_handler.setLevel(level)
        logger.addHandler(stream_handler)

    if file_log:
        file_path = log_file or DEFAULT_LOG_FILE
        log_dir = os.path.dirname(file_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(file_path, mode='a', encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)

    return logger


if __name__ == '__main__':
    logging.info("test-info")
    logging.error("test-error")
    logging.warning("test-warning")
