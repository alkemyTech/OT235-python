import logging
from logging import config

#de donde toma la configuracion
config.fileConfig("logger_big_data.cfg")

#asigno los logger
logger = logging.getLogger("root")


if __name__ == "__main__":
    #un mensaje para probar
    logger.info("Salida del logger")