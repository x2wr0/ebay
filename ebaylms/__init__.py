#! -*- coding: utf-8 -*-

# o5/2o2o: 0.24.7              :: wro-guys


import logging
from requests import Request, Session
from requests.adapters import HTTPAdapter
import time


url_filetransfer = 'storage.ebay.com/FileTransferService'
url_bulkexchange = 'webservices.ebay.com/BulkDataExchangeService'

api_version = 1163
side_id = 77

HTTP_SSL = {False: 'http',
            True: 'https'}


def get_logger(name, levelname='info'):
    logger = logging.getLogger(name)
    formatter = logging.Formatter('%(name)s [%(levelname)s]  %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    level = logging.CRITICAL
    if levelname == 'debug':
        level = logging.DEBUG
    if levelname == 'info':
        level = logging.INFO
    if levelname == 'warning':
        level = logging.WARNING
    if levelname == 'error':
        level = logging.ERROR
    logger.setLevel(level)
    return logger


# class BaseConnection:
#     '''interface for basic LMS-connection set up'''
#     def __init__(self, debug=False, method='GET', proxy_host=None, proxy_port=80, timeout=60, escape_xml=False,
#                  parallel=None, **kwargs):
#         self.response = None
#         self.request = None
#         self.verb = None
#         self.config = None
#         self.debug = debug
#         self.method = method
#         self.timeout = timeout
#         self.proxy_host = proxy_host
#         self.proxy_port = proxy_port
#         self.escape_xml = escape_xml
#         self.parallel = parallel
#         self.base_list_nodes = []
#         self.datetime_nodes = []
#         self._list_nodes = []
#         self.proxies = dict()
#
#         if self.proxy_host:
#             proxy = 'http://{}:{}'.format(self.proxy_host, self.proxy_port)
#             self.proxies.update({'http': proxy, 'https': proxy})
#
#         self.session = Session()
#         self.session.mount('http://', HTTPAdapter(max_retries=3))
#         self.session.mount(('https://', HTTPAdapter(max_retries=3)))
#
#         self._reset()
#
#     def _reset(self):
#         self.response = None
#         self.request = None
#         self.verb = None
#         self._list_nodes = []
#         self._request_id = None
#         self._request_dict = {}
#         self._time = time.time()
#         self._response_content = None
#         self._response_dom = None
#         self._response_obj = None
#         self._response_soup = None
#         self._response_dict = None
#         self._response_error = None
#         self._resp_body_errors = []
#         self._resp_body_warnings = []
#         self._resp_codes = []
