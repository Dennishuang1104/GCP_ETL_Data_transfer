from xml.etree import ElementTree
import requests


class APICallerTools:
    def __init__(self):
        self.apiURL = ""
        self.params = {}
        self.headers = {}

    def reset(self):
        self.apiURL = ""
        self.params.clear()
        self.headers.clear()

    def set_url(self, _apiURL):
        self.apiURL = _apiURL

    def insert_header(self, _sendHeaderKey, _sendHeaderVal):
        self.headers[_sendHeaderKey] = _sendHeaderVal

    def insert_param(self, _sendParamKey, _sendParamVal):
        self.params[_sendParamKey] = _sendParamVal

    def call_api(self, _method, _return=""):
        if _method == "GET":
            result = requests.get(self.apiURL, params=self.params, headers=self.headers)
        elif _method == "POST":
            result = requests.post(self.apiURL, data=self.params, headers=self.headers)
        if _return == "json":
            return result.json()
        elif _return == "xml":
            return ElementTree.fromstring(result.content)
        return result.content