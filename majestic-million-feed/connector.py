"""
Copyright start
MIT License
Copyright (c) 2023 Fortinet Inc
Copyright end
"""

from connectors.core.connector import Connector, get_logger, ConnectorError
from .operations import operations, _check_health

logger = get_logger('majestic-million-feed')


class Majestic(Connector):

    def execute(self, config, operation, params, **kwargs):
        try:
            action = operations.get(operation)
            return action(config, params)
        except Exception as err:
            logger.exception(str(err))
            raise ConnectorError(str(err))

    def check_health(self, config):
        try:
            logger.debug(f" check_health() executing {config}")
            status = _check_health(config)
            logger.info("status: check_health() excecuted ")
            return status
        except Exception as err:
            logger.exception(str(err))
            raise ConnectorError(str(err))
