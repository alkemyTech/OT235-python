import logging.config
import logging.handlers

logging.config.fileConfig('loggerBD.cfg')

log = logging.getLogger('root')

if __name__ == '__main__':
   log.info('msg')