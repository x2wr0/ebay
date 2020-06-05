#! -*- coding: utf-8 -*-

# o5/2o2o: 0.24.7              :: wro-guys


import gzip
from base64 import standard_b64encode as b64encode
#from base64 import urlsafe_b64encode as b64encode

from . import api_version
# from libdrebo.utils import to_bytes, Item


def to_bytes(bytes_or_str):
	if isinstance(bytes_or_str, str):
		value = bytes_or_str.encode('utf-8')
	else:
		value = bytes_or_str
	return value


class Item:
	def __init__(self, **kwargs):
		self._xxx = []   # Fehler Log
		for key in kwargs:
			self.__dict__[key] = kwargs[key]

	def _get_xxx(self):
		return ' - '.join(self._xxx)
	def _set_xxx(self, xxx):
		self._xxx.append(xxx)
	xxx = property(_get_xxx, _set_xxx)


class BulkData:
	"""collection of upload job data

	ReviseFixedPriceItem- / ReviseInventoryStatus-Data for
	inserting into BulkDataExchangeRequest
	"""
	def __init__(self, **kwargs):
		self._bdes_list = ['ReviseFixedPriceItem', 'ReviseInventoryStatus']
		self._data = Item(**{key: [] for key in self._bdes_list})
		#self._data = []
		self.fileReferenceId = kwargs.get('fileReferenceId', None)
		self.jobId = kwargs.get('jobId', None)
		self.jobType = kwargs.get('jobType', None)
		self._bder = None
		self._reviseType = None
		self.version = kwargs.get('version', api_version)
		self.site_id = kwargs.get('site_id', 77)

	def _get_bder(self):
		return self._bder
	bder = property(_get_bder)

	def _get_bder_compressed(self):
		if self._bder:
			return b64encode(gzip.compress(to_bytes(self._bder)))
	bder_compressed = property(_get_bder_compressed)

	def add_item(self, item):
		"""add an EbayItem"""
		# TODO: generalize "revise"Type/Data. could be e.g. AddItem..
		if item.reviseType:
			key = 'Revise{:s}'.format(item.reviseType)
			self._data.__dict__[key].append(item)

	def add_items(self, items):
		"""takes a list of EbayItems and adds them"""
		for item in items:
			self.add_item(item)

	def create_bder(self, **kwargs):
		"""create the BulkDataExchangeRequests content"""
		verb = kwargs.get('jobType', self.jobType)
		xml_enclosure = '<>{}</>'
		if verb in self._bdes_list:
			if verb == 'ReviseFixedPriceItem':
				xml_enclosure = '<Item>{}</Item>'
			if verb == 'ReviseInventoryStatus':
				xml_enclosure = '<InventoryStatus>{}</InventoryStatus>'
			xmlns = 'xmlns="urn:ebay:apis:eBLBaseComponents"'
			xml = '<?xml version="1.0" encoding="utf-8"?>'   # --> filedata
			xml += '<BulkDataExchangeRequests>'
			xml += '<Header><Version>{}</Version>'.format(self.version)
			xml += '<SiteID>{}</SiteID></Header>'.format(self.site_id)
			data = self._data.__dict__[verb]
			# TODO: generalize "revise"Type/Data. could be e.g. AddItem..
			xml_request_open = '<{}Request {}><Version>{}</Version>'.format(verb, xmlns, self.version)
			xml_request_close = '</{}Request>'.format(verb)
			n = 0
			m = 4
			items = [xml_enclosure.format(item.reviseData) for item in data]
			last = len(items) - 1
			for item in items:
				if n % m == 0:
					xml += xml_request_open
				if n % m >= 0:
					xml += item
				if n % m == m-1 or n == last:
					xml += xml_request_close
				n += 1
			xml += '</BulkDataExchangeRequests>'             # <-- filedata
		else:
			raise NotImplementedError
		self._bder = xml
		#return xml
