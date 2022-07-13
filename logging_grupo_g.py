#importamos logging y config
import logging
import logging.config


#Implementacion del log
logging.config.fileConfig('logs_grupo_g.cfg')
logger = logging.getLogger()

logger.debug('message')