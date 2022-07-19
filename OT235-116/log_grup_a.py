import logging.config
import logging.handlers
from os import path

# We specify the working path since logging works from the root not where the .py file is executed
log_file_path = path.join(path.dirname(path.abspath(__file__)), 'log_config_a.cfg')
logging.config.fileConfig(log_file_path)

logger = logging.getLogger('root')

if __name__ == '__main__':
   logger.debug('This is an information message')