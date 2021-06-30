# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import requests
import logging
from ..units import Units

log = logging.getLogger(__name__)


class Wunderground:
    endpoint_url = 'https://api.weather.com'
    rest_user_agent = 'WundergroundAPI/0.1'
    timeout = 5.0

    def __init__(self, api_key: str, unit: str):
        self._api_key = api_key
        if unit not in Units.UNITS:
            raise ValueError("Unknown unit %s!" % unit)
        self._unit = unit

    def _setup_client(self):
        headers = {
            'Accept': 'application/json',
            'User-Agent': self.rest_user_agent,
        }

        s = requests.Session()
        s.headers.update(headers)

        return s
