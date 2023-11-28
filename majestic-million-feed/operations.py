"""
Copyright start
MIT License
Copyright (c) 2023 Fortinet Inc
Copyright end
"""

import requests
from connectors.core.connector import get_logger, ConnectorError
from django.conf import settings
import os
import polars as pl
from io import StringIO

logger = get_logger('majestic-million-feed')


class MajesticMillion:
    def __init__(self, config):
        self.verify_ssl = config.get('verify_ssl')

    def make_request(self, endpoint, method='GET', data=None, params=None, files=None):
        try:
            url = endpoint
            logger.info('Executing url {}'.format(url))

            response = requests.request(method, url, params=params, files=files, data=data, headers=params,
                                        verify=self.verify_ssl)
            if response.ok:
                logger.info('successfully get response for url {}'.format(url))
                return response
            elif response.status_code == 400:
                error_response = response.json()
                error_description = error_response['message']
                raise ConnectorError({'error_description': error_description})
            elif response.status_code == 401:
                error_response = response.json()
                if error_response.get('error'):
                    error_description = error_response['error']
                else:
                    error_description = error_response['message']
                raise ConnectorError({'error_description': error_description})
            elif response.status_code == 404:
                error_response = response.json()
                error_description = error_response['message']
                raise ConnectorError({'error_description': error_description})
            else:
                logger.error(response.json())
        except requests.exceptions.SSLError:
            raise ConnectorError('SSL certificate validation failed')
        except requests.exceptions.ConnectTimeout:
            raise ConnectorError('The request timed out while trying to connect to the server')
        except requests.exceptions.ReadTimeout:
            raise ConnectorError('The server did not send any data in the allotted amount of time')
        except requests.exceptions.ConnectionError:
            raise ConnectorError('Invalid endpoint or credentials')
        except Exception as err:
            raise ConnectorError(str(err))
        raise ConnectorError(response.text)


def download_domains_csv(config, params):
    try:
        logger.info("Before Starting action")
        mm = MajesticMillion(config)
        params = _build_payload(params)
        endpoint = "https://downloads.majestic.com/majestic_million.csv"

        response = mm.make_request(endpoint=endpoint, method='GET').content
        path = os.path.join(settings.TMP_FILE_ROOT, "majestic_million.csv")

        with open(path, 'wb') as fp:
            fp.write(response)
        return {"Path": path, "Status": "Successfully Downloaded"}
    except Exception as err:
        logger.error(str(err))
        raise ConnectorError(str(err))


def get_domain_records(config, params):
    try:
        mm = MajesticMillion(config)
        params = _build_payload(params)
        endpoint = "https://downloads.majestic.com/majestic_million.csv"

        res = mm.make_request(endpoint=endpoint, method="GET").text
        csv_file = StringIO(res)
        df = pl.read_csv(csv_file, n_rows=params.get("limit"))

        list_of_dicts = [
            {col: df[col][i] for col in df.columns} for i in range(len(df))
        ]
        return list_of_dicts
    except Exception as err:
        logger.error(str(err))
        raise ConnectorError(str(err))


def _check_health(config):
    try:
        return config is not None
    except Exception as err:
        logger.error(str(err))
        raise ConnectorError(str(err))


def _build_payload(params):
    return {key: val for key, val in params.items() if val is not None and val != ''}


operations = {
    "download_domains_csv": download_domains_csv,
    "get_domain_records": get_domain_records
}
