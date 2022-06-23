
###################
# SETUP LOGGER
###################

import logging


def create_logger(task):
    """Create logger

    Args:
        task (string): task name

    Returns:
        logger: logger object

    # example usage
    from logger import create_logger
    logger = create_logger()
    logger.info('info message')
    """

    # create logger
    logger = logging.getLogger(task)
    # set log level for all handlers to debug
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    # best for development or debugging
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    consoleHandler.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(consoleHandler)

    return logger
