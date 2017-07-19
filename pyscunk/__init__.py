import requests
import pandas as pd
import json
from xml.etree import ElementTree
from datetime import datetime, timedelta

_base_path_templ = '{proto}://{hst}:{prt}/'
_auth_req = '/services/auth/login'
_search_req = '/services/search/jobs'
_search_path = '/servicesNS/{owner}/{app}/search/jobs'

_def_proto = 'https'
_def_host = 'localhost'
_def_port = 8089
_def_session_ttl = 30

__version__ = 0.1


def _get_splunk_auth_token(user, password, host=_def_host, port=_def_port, app='search'):
    auth_data = {'username': user, 'password': password, 'cookie': 1}
    auth_req = _base_path_templ.format(proto=_def_proto, hst=host, prt=port) + _auth_req
    r = requests.post(auth_req, data=auth_data, verify=False)
    if r.status_code == 200:
        doc = ElementTree.fromstring(r.text)
        try:
            token = doc.find('sessionKey').text
            return token
        except Exception as e:
            ValueError('Unable to extract Splunk session token: {} {}'.format(r.status_code, r.text))
    else:
        raise ValueError('Unable to connect to Splunk: {} {}'.format(r.status_code, r.text))


class SplunkConnector():
    """
    Encapsulates a Splunk session
    Currently with basic oneshot (blocking) query

    """
    def __init__(self, user, passwd, host, port, app, session_token, create_time=datetime.now()):
        self._session_token = session_token
        self._last_token_use_time = create_time
        self._host = host
        self._port = port
        self._user = user
        self.__passwd = passwd
        self._app = app
        self._base_path = _base_path_templ.format(proto='https', hst=self._host, prt=self._port)


    def get_session_token(self):
        return self._session_token

    def query(self, q, earliest, latest=datetime.now()):
        """
        Blocking Splunk search
        :param q: The splunk search query string
        :param earliest: from when
        :param latest: to when
        :return:
        """
        latest = datetime.now()
        if (self._last_token_use_time + timedelta(minutes=30)) < latest:
            self._session_token = _get_splunk_auth_token(user=self._user, password=self.__passwd, host=self._host, port=self._port, app=self._app)
        self._last_token_use_time = latest

        query_params = {'search': q,
                        'exec_mode': 'oneshot',
                        'output_mode': 'json',
                        'count' : '50000',
                        'earliest_time': earliest.isoformat(),
                        'latest_time': latest.isoformat()}

        headers = {'Authorization': 'Splunk ' + self._session_token}
        request_path = self._base_path + _search_path.format(owner=self._user, app=self._app)
        r = ''
        try:
            r = requests.post(request_path, headers=headers, data=query_params, verify=False)
            return r.text
        except Exception as e:
            print('Exception occured whilst querying Splunk:\n{}').format(e.message)

    def search(self, q, earliest, latest):
        """
        Search Splunk and get a Pandas DataFrame in return
        :param q:    Splunk search expressed in Splunk Query Language
        :param earliest: from datetime
        :param latest:  to datetime
        :return: DataFrame of results
        """
        result_raw = self.query(q,earliest, latest)
        return pd.DataFrame([r for r in json.loads(result_raw)['results']])

    def search_retry(self, q, earliest, latest, max_retry=3):
        trys = 0
        while trys < max_retry:
            try:
                df = self.search(q,earliest,latest)
                return df
            except Exception as e:
                trys = trys + 1
                if trys >= max_retry:
                    raise e


def connect(user, password, host=_def_host, port=_def_port, app='search'):
    """
    Connects to the Splunk API
    :param user:  The Splunk user credential
    :param password: Password
    :param host: The Splunk API host
    :param port:
    :param app: The default Splunk application context
    :return: SplunkConnector instance
    """
    token = _get_splunk_auth_token(user, password, host, port, app)
    return SplunkConnector(user=user, passwd=password, host=host, port=port, app=app, session_token=token)
