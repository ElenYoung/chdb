import logging
import colorlog
import coloredlogs
import os

# 默认日志文件
DEFAULT_LOG_FILE = "default_log.log"

#默认格式
DEFAULT_LOG_FORMAT =  "%(asctime)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

def get_logger(name=None, log_file=None, level=logging.DEBUG, terminal=False, file_log=True):
    """获取一个日志记录器，并支持自定义日志文件和是否在终端输出"""
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger  # 防止重复添加处理器

    logger.setLevel(level)

    # 创建格式化器
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT)

    # 配置彩色日志的样式
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

    # 只有在 terminal=True 时才添加终端处理器
    if terminal:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(colored_formatter)
        stream_handler.setLevel(level)
        logger.addHandler(stream_handler)

    # 设置文件处理器（始终写入文件）
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
