import Environment
import logging
import os


class Logger:
    def __init__(self, logging_level, abspath: str):
        """
        :param logging_level:
        :param abspath: all abspath should dependence under Log dir
        """
        if '/' in abspath:
            dir_path = '/'.join(abspath.split('/')[0:-1])
            if not os.path.exists(Environment.LOG_DIR + dir_path):
                os.mkdir(Environment.LOG_DIR + dir_path)

        if not os.path.exists(Environment.LOG_DIR + abspath):
            with open(Environment.LOG_DIR + abspath, 'w') as f:
                pass

        logging.basicConfig(
            level=logging_level,
            format=Environment.LOGGING_FORMAT,
            datefmt=Environment.LOGGING_DATEFMT,
            filename=Environment.LOG_DIR + abspath
        )

    @staticmethod
    def debug(content):
        logging.debug(content, exc_info=True)

    @staticmethod
    def info(content):
        logging.info(content, exc_info=True)

    @staticmethod
    def warning(content):
        logging.warning(content, exc_info=True)

    @staticmethod
    def error(content):
        logging.error(content, exc_info=True)

    @staticmethod
    def exception(content):
        logging.critical(content, exc_info=True)
