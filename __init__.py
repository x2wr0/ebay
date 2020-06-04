#! -*- coding: utf-8 -*-

# o3/2o2o: 0.1   


from uuid import uuid4

from ebaysdk import log
from ebaysdk.connection import BaseConnection
from ebaysdk.config import Config


url_filetransfer = 'storage.ebay.com/FileTransferService'
url_bulkexchange = 'webservices.ebay.com/BulkDataExchangeService'

api_version = 1149


class Connection(BaseConnection):
	"""Large Merchant Services Base Connection Class
	
	API documentation:
	https://developer.ebay.com/DevZone/large-merchant-services/Concepts/MakingACall.html
	
	Supported calls:
	createUploadJob
	uploadFile
	*[ReviseInventoryStatus]*
	*[ReviseFixedPriceItem]*
	startUploadJob
	getJobs
	getJobStatus
	abortJob
	"""
	def __init__(self, **kwargs):
		super(Connection, self).__init__(method='POST', **kwargs)
		self.config = Config(
			domain=kwargs.get('domain', 'webservices.ebay.com'),
			connection_kwargs=kwargs,
			config_file=kwargs.get('config_file', 'ebay.yaml'))
		self.config.set('domain', kwargs.get('domain', 'webservices.ebay.com'))
		self.config.set('content_type', 'text/xml;charset=UTF-8')
		self.config.set('request_encoding', 'XML')
		self.config.set('response_encoding', 'XML')
		#self.datetime_nodes = ['endtimefrom', 'endtimeto', 'timestamp']
		#self.base_list_nodes = []
		self.uuid = uuid4()
		self.file_type = self.config.get('file_type', 'gzip')
		self.version = self.config.get('version', api_version)
		self.site_id = self.config.get('site_id', '77')

	def build_request_url(self, verb):
		if verb == 'uploadFile':
			url = 'https://{:s}'.format(url_filetransfer)
		else:
			url = 'https://{:s}'.format(url_bulkexchange)
		return url

	def build_request_headers(self, verb):
		headers = {
			'Content-Type': self.config.get('content_type'),
			'X-EBAY-SOA-SECURITY-TOKEN': self.config.get('token'),
			'X-EBAY-SOA-OPERATION-NAME': verb}
			#'X-EBAY-SOA-GLOBAL-ID': self.config.get('site-id'),
			#'X-EBAY-REQUEST-DATA-FORMAT': self.config.get('request_encoding'),
			#'X-EBAY-RESPONSE-DATA-FORMAT': self.config.get('response_encoding'),
			#'X-EBAY-SOA-MESSAGE-PROTOCOL': self.config.get('message_protocol')}
		if verb == 'uploadFile':
			headers['X-EBAY-SOA-SERVICE-NAME'] = 'FileTransferService'
		if verb == 'ReviseFixedPriceItem' or verb == 'ReviseInventoryStatus':
			headers['X-EBAY-SOA-SERVICE-NAME'] = 'BulkDataExchangeService'
		return headers

	def build_request_data(self, verb, data=None, verb_attrs=None):
		xmlns = 'xmlns="http://www.ebay.com/marketplace/services"'
		xmlns_sct = 'xmlns:sct="http://www.ebay.com/soaframework/common/types"'
		xml = '<?xml version="1.0" encoding="utf-8"?>'
		# createUploadJobRequest
		if verb == 'createUploadJob':
			xml += '<{}Request {}>'.format(verb, xmlns)
			xml += '<uploadJobType>{}</uploadJobType>'.format(data)  # '<jobType>'
			xml += '<UUID>{}</UUID>'.format(self.uuid)
			xml += '<fileType>{}</fileType>'.format(self.file_type)  # 'gzip'
			xml += '</{}Request>'.format(verb)

		# uploadFileRequest
		if verb == 'uploadFile':
			xml += '<{}Request {} {}>'.format(verb, xmlns, xmlns_sct)
			xml += '<taskReferenceId>{}</taskReferenceId>'.format(data.jobId)  # BulkData.jobId
			xml += '<fileReferenceId>{}</fileReferenceId>'.format(data.fileReferenceId)  # BulkData.fileReferenceId
			xml += '<fileFormat>{}</fileFormat>'.format(self.file_type)
			xml += '<fileAttachment>'
			xml += '<Data>{}</Data>'.format(data.bder_compressed)              # BulkData.bder_compressed
			xml += '</fileAttachment>'
			xml += '</{}Request>'.format(verb)

		# startUploadJobRequest
		if verb == 'startUploadJob':
			xml += '<{}Request {}>'.format(verb, xmlns)
			#xml += '<UUID>{}</UUID>'.format(self.uuid)
			xml += '<jobId>{}</jobId>'.format(data.jobId)  # BulkData.jobId
			xml += '</{}Request>'.format(verb)

		# getJobsRequest
		if verb == 'getJobs':
			xml += '<{}Request {}>'.format(verb, xmlns)
			# options..
			xml += '</{}Request>'.format(verb)

		# getJobStatusRequest
		if verb == 'getJobStatus':
			xml += '<{}Request {}>'.format(verb, xmlns)
			xml += '<jobId>{}</jobId>'.format(data.jobId)  # BulkData.jobId
			xml += '</{}Request>'.format(verb)

		# abortJobRequest
		if verb == 'abortJob':
			xml += '<{}Request {}>'.format(verb, xmlns)
			xml += '<jobId>{}</jobId>'.format(data.jobId)  # BulkData.jobId
			xml += '</{}Request>'.format(verb)

		return xml

	def _get_warnings(self):
		warning_string = ''
		if len(self._resp_body_warnings) > 0:
			warning_string = '{:s}: {:s}'.format(self.verb, ', '.join(self._resp_body_warnings))
		return warning_string
	warnings = property(_get_warnings)

	def _get_resp_body_errors(self):
		if self._resp_body_errors and len(self._resp_body_errors) > 0:
			return self._resp_body_errors
		errors = []
		warnings = []
		resp_codes = []

		if self.verb is None:
			return errors

		dom = self.response.dom()
		if dom is None:
			return errors

		for e in dom.findall('error'):
			eSeverity = None
			eDomain = None
			eMsg = None
			eId = None

			try:
				eSeverity = e.findall('severity')[0].text
			except IndexError:
				pass
			try:
				eDomain = e.findall('domain')[0].text
			except IndexError:
				pass
			try:
				eMsg = e.findall('message')[0].text
			except IndexError:
				pass
			try:
				eId = e.findall('errorId')[0].text
				if int(eId) not in resp_codes:
					resp_codes.append(int(eId))
			except IndexError:
				pass

			msg = 'Domain: {:s}, Severity: {:s}, errorId: {:s}, {:s}'.format(
				eDomain, eSeverity, eId, eMsg)

			if eSeverity == 'Warning':
				warnings.append(msg)
			else:
				errors.append(msg)

		self._resp_body_warnings = warnings
		self._resp_body_errors = errors
		self._resp_codes = resp_codes

		if self.config.get('warnings') and len(warnings) > 0:
			log.warn('{:s}: {:s}\n\n'.format(self.verb, '\n'.join(warnings)))

		try:
			if self.response.reply.ack == 'Success' and len(errors) > 0 and self.config.get('errors'):
				log.error('{:s}: {:s}\n\n'.format(self.verb, '\n'.join(errors)))
			elif len(errors) > 0:
				if self.config.get('errors'):
					log.error('{:s}: {:s}\n\n'.format(self.verb, '\n'.join(errors)))
				return errors
		except AttributeError:
			pass

		return []
