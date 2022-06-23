# importacion de libreria

import logging

# configuracion del logs
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d')

logger=logging.getLogger('logger')

handlerConsola=logging.StreamHandler()

logger.addHandler(handlerConsola)

# creacion de mensajes
logger.warning('alerta')

logger.debug('nivel debug')

