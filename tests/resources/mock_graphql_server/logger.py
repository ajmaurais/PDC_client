
import logging

logging.basicConfig(
    level=logging.WARNING ,
    format='%(asctime)s - %(filename)s %(funcName)s:%(lineno)d - %(levelname)s: %(message)s'
)
LOGGER = logging.getLogger()