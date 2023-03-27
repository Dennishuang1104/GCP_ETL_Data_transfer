import abc
from typing import Dict, Union
import requests as req
from enum import IntEnum
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class AdaptorMode(IntEnum):
    DEFAULT = -1
    GET = 0
    POST = 1
    DELETE = 2


class ReqAdaptor(metaclass=abc.ABCMeta):

    def __init__(self):
        self.url: str = ''
        self.timeout = 20
        self.mode: AdaptorMode = AdaptorMode.DEFAULT
        self._headers: Dict = {}
        self._payload: Union[str, Dict] = ''
        self.verify = True
        self._session: req.Session = req.session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

        self.__func_map = {
            AdaptorMode.GET: self.__get,
            AdaptorMode.POST: self.__post,
            AdaptorMode.DELETE: self.__delete
        }

        self._response: req.Response = req.Response()
        self.extractor_code: str = ''

    def __get(self):
        try:
            self.add_to_headers(
                field='User-Agent',
                value='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/81.0.4044.138 Safari/537.36'
            )

            self._response = self._session.get(
                url=self.url,
                headers=self._headers,
                params=self._payload,
                timeout=self.timeout,
                verify=self.verify
            )
        except:
            raise

    def __post(self):
        try:
            self.add_to_headers(
                field='User-Agent',
                value='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko)'
                      ' Chrome/81.0.4044.138 Safari/537.36'
            )

            self._response = self._session.post(
                url=self.url,
                headers=self._headers,
                data=self._payload,
                timeout=self.timeout,
                verify=self.verify
            )
        except:
            raise

    def __delete(self):
        try:
            self.add_to_headers(
                field='User-Agent',
                value='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko)'
                      ' Chrome/81.0.4044.138 Safari/537.36'
            )

            self._response = self._session.delete(
                url=self.url,
                headers=self._headers,
                data=self._payload
            )
        except:
            raise

    def exec(self):
        try:
            self.__func_map[self.mode]()
        except:
            raise

    @property
    def headers(self) -> Dict:
        return self._headers

    def add_to_headers(self, field, value):
        self._headers[field] = value

    @property
    def payload(self) -> Union[str, Dict]:
        return self._payload

    @payload.setter
    def payload(self, payload):
        try:
            self._payload = payload
        except:
            raise

    @property
    def sess(self) -> req.Session:
        return self._session

    @property
    def response_data(self):
        return self._response

    @property
    def GET_MODE(self):
        return AdaptorMode.GET

    @property
    def POST_MODE(self):
        return AdaptorMode.POST

    @property
    def DELETE_MODE(self):
        return AdaptorMode.DELETE
