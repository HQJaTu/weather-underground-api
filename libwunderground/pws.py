# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import logging
from datetime import datetime, date
from .abstract import Wunderground

log = logging.getLogger(__name__)


class PWS(Wunderground):

    def __init__(self, api_key: str, unit: str, pws_id: str):
        super().__init__(api_key=api_key, unit=unit)
        self._pws_id = pws_id

    def get_historical_data(self, day: date):
        """
        https://www.wxforum.net/index.php?topic=37505.0
        """
        http_client = self._setup_client()
        date_str = day.strftime("%Y%m%d")
        url = "%s/v2/pws/history/all?stationId=%s&format=json&units=%s&date=%s&apiKey=%s" % (
            self.endpoint_url, self._pws_id, self._unit, date_str, self._api_key
        )
        log.debug("PWS get URL: %s" % url)
        response = http_client.get(url, timeout=self.timeout)
        if response.status_code != 200:
            if response.status_code == 204:
                log.warning("Requested data for day %s, but it is not available." % day)
                return None
            log.error("Error: %s" % response.content.decode('UTF-8'))
            raise RuntimeError("Failed to request data from Weather.com API! HTTP/%d" %
                               response.status_code)

        data = response.json()
        if not data['observations']:
            return None

        return data['observations']
