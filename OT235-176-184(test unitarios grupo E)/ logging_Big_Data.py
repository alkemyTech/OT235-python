import logging
from logging.config import fileConfig

# log implementation
CONFIG_FILE = 'log_Big_Data.cfg'
fileConfig(CONFIG_FILE)

logger = logging.getLogger()
logger.debug('log de prueba')