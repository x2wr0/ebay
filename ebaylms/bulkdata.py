#! -*- coding: utf-8 -*-

# o5/2o2o: 0.24.7              :: wro-guys


import gzip
from base64 import encodebytes
# from base64 import standard_b64encode as encodebytes
# from base64 import urlsafe_b64encode as encodebytes

from ebaysdk.utils import dict2xml

from . import api_version, side_id
# from ebay import api_version, side_id


class BulkData:
    """collection of upload job data

    ReviseFixedPriceItem- / ReviseInventoryStatus-Data for
    inserting into BulkDataExchangeRequest
    """
    _bdes_keys = ['ReviseFixedPriceItem', 'ReviseInventoryStatus']

    def __init__(self, **kwargs):
        self.jobType = kwargs.get('jobType', None)
        self.jobId = kwargs.get('jobId', None)
        self.fileReferenceId = kwargs.get('fileReferenceId', None)
        self.version = kwargs.get('version', api_version)
        self.site_id = kwargs.get('site_id', side_id)
        self._warnings = bool(kwargs.get('warnings', False))
        self._data = []

    def _get_warnings(self):
        return self._warnings

    def _set_warnings(self, value):
        if not isinstance(value, bool):
            value = bool(value)
        self._warnings = value

    warnings = property(_get_warnings, _set_warnings)

    def _get_bder(self):
        verb = self.jobType
        xmlns = 'xmlns="urn:ebay:apis:eBLBaseComponents"'
        xml = '<?xml version="1.0" encoding="utf-8"?>\n'   # --> filedata
        xml += '<BulkDataExchangeRequests>\n'
        xml += '<Header>\n<Version>{}</Version>\n'.format(self.version)
        xml += '<SiteID>{}</SiteID>\n</Header>\n'.format(self.site_id)
        if verb:
            if verb in BulkData._bdes_keys:
                xml_request_open = '<{}Request {}>\n<Version>{}</Version>\n'.format(verb, xmlns, self.version)
                xml_request_close = '</{}Request>\n'.format(verb)
                if verb == 'ReviseFixedPriceItem':
                    xml_enclosure = '<Item>\n{}\n</Item>\n'
                elif verb == 'ReviseInventoryStatus':
                    xml_enclosure = '<InventoryStatus>\n{}</InventoryStatus>\n'
                else:
                    xml_enclosure = '{}'
                if self._warnings:
                    xml_request_open += '<WarningLevel>High</WarningLevel>\n'
                    for data in self._data:
                        xml += xml_request_open
                        xml += xml_enclosure.format(dict2xml(data))
                        xml += '<MessageID>{}</MessageID>\n'.format(data['ItemID'])
                        xml += xml_request_close
                else:
                    m0 = 4  # max 4 items per request
                    m1 = m0 - 1
                    last = len(self._data) - 1
                    for n, data in enumerate(self._data):
                        if n % m0 == 0:
                            xml += xml_request_open
                        xml += xml_enclosure.format(dict2xml(data))
                        if n % m0 == m1 or n == last:
                            xml += xml_request_close
            elif verb == 'EndFixedPriceItem':
                xml_request_open = '<{}Request {}>\n'.format(verb, xmlns, self.version)
                xml_request_close = '</{}Request>\n'.format(verb)
                for data in self._data:
                    xml += xml_request_open
                    xml += '<Version>{}</Version>'.format(self.version)
                    xml += '<ItemID>{}</ItemID>'.format(data['ItemID'])
                    xml += '<SKU>{}</SKU>'.format(data['SKU'])
                    xml += '<EndingReason>NotAvailable</EndingReason>'
                    xml += xml_request_close
            else:
                raise NotImplementedError
        else:
            xml += '<!--\nnothing to say.. no content given.\ninsert some data to show:\n' \
                   '">>> BulkData.add_item(EbayItem) or BulkData.add_items([EbayItem,..])\n-->"\n'
        xml += '</BulkDataExchangeRequests>'               # <-- filedata
        return xml

    bder = property(_get_bder)

    def _get_bder_compressed(self):
        """when there is some "real" content (._data contains at least a list of dicts),
        return the compressed and base64 encoded request"""
        if len(self._data) > 0:
            return encodebytes(gzip.compress(self._get_bder().encode('utf-8'))).decode('utf-8')

    bder_compressed = property(_get_bder_compressed)

    def add_item(self, item):
        """add an EbayItem.data field to the ._data list"""
        self._data.append(item.data)

    def add_items(self, items):
        """takes a list of EbayItems and adds them to the ._data list"""
        for item in items:
            self.add_item(item)
