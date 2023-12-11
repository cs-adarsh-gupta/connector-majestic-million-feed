"""
Copyright start
MIT License
Copyright (c) 2023 Fortinet Inc
Copyright end
"""

import requests, json
from connectors.core.connector import get_logger, ConnectorError
from django.conf import settings
import os
import polars as pl
from integrations.crudhub import make_request
from requests import request, post, exceptions as req_exceptions
from io import StringIO


logger = get_logger('majestic-million-feed')


try:
    from integrations.crudhub import trigger_ingest_playbook
except:
    # Ignore. Lower FSR version
    pass


class MajesticMillion:
    def __init__(self, config):
        self.server_url = "https://downloads.majestic.com"
        self.verify_ssl = config.get('verify_ssl')

    def make_request(self, endpoint, method='GET', data=None, params=None, files=None):
        url = self.server_url + endpoint
        logger.info('Executing URL: {}'.format(url))
        try:
            response = requests.request(method, url, params=params, files=files, data=data, headers=params,
                                        verify=self.verify_ssl)
            if response.ok:
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


def _upload_file_to_cyops(file_name, file_content, file_type):
    try:
        # Conditional import based on the FortiSOAR version.
        try:
            from integrations.crudhub import make_file_upload_request
            response = make_file_upload_request(file_name, file_content, 'application/octet-stream')
        except:
            from cshmac.requests import HmacAuth
            from integrations.crudhub import maybe_json_or_raise

            url = settings.CRUD_HUB_URL + '/api/3/files'
            auth = HmacAuth(url, 'POST', settings.APPLIANCE_PUBLIC_KEY,
                            settings.APPLIANCE_PRIVATE_KEY,
                            settings.APPLIANCE_PUBLIC_KEY.encode('utf-8'))
            files = {'file': (file_name, file_content, file_type, {'Expire': 0})}
            response = post(url, auth=auth, files=files, verify=False)
            response = maybe_json_or_raise(response)

        file_id = response['@id']
        file_description = 'Feeds retrieved from Majestic Million Feed.'
        attach_response = make_request('/api/3/attachments', 'POST',
                                       {'name': file_name, 'file': file_id, 'description': file_description})
        logger.debug('attach file complete: {0}'.format(attach_response))
        return attach_response
    except Exception as err:
        logger.exception('An exception occurred {0}'.format(str(err)))
        raise ConnectorError('An exception occurred {0}'.format(str(err)))


def _create_cyops_attachment(file_name, content):
    attachment_name = file_name
    file_resp = _upload_file_to_cyops(attachment_name, content, 'application/octet-stream')
    return file_resp


def get_domain_records(config, params, **kwargs):
    try:
        mm = MajesticMillion(config)
        params = _build_payload(params)
        endpoint = "/majestic_million.csv"
        response = mm.make_request(endpoint=endpoint, method='GET')
        process_response_as = params.get("process_response_as")
        if process_response_as in ["Return as JSON", "Create as Feed Records in FortiSOAR"]:
            res = response.text
            csv_file = StringIO(res)
            df = pl.read_csv(csv_file, n_rows=params.get("limit") if params.get("limit") else None)
            list_of_dicts = [
                {col: df[col][i] for col in df.columns} for i in range(len(df))
            ]
            if process_response_as == "Return as JSON":
                return list_of_dicts
            else:
                # Create as Feed Records in FortiSOAR
                create_pb_id = params.get('create_pb_id')
                confidence = params.get('confidence')
                reputation = params.get('reputation')
                tlp = params.get('tlp')
                playbook_params = {"confidence": confidence, "reputation": reputation, "tlp": tlp}
                trigger_ingest_playbook(list_of_dicts, create_pb_id, parent_env=kwargs.get('env', {}),
                                        batch_size=1000, pb_params=playbook_params)
                return "Successfully triggered playbooks to create feed records."
        else:
            # Download and Create CSV Attachment in FortiSOAR
            res = response.content
            file_name = "majestic_million_feed.csv"
            return _create_cyops_attachment(file_name=file_name, content=res)
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
    "get_domain_records": get_domain_records
}
