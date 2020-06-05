#! -*- coding: utf-8 -*-

from libdrebo.shop import Shop, ShopProfile

from . import Connection as Merchant
from .accessories import EbaySellerList
from .bulkdata import BulkData

shop = Shop(ShopProfile('ps'))
sellerlist = EbaySellerList(shop=shop, debug=True)
api = Merchant(debug=False)

job_ok = False
job_type = 'ReviseInventoryStatus'
#job_type = 'ReviseFixedPriceItem'

print('\n:: jobType: %s..' % job_type)
api.execute('getJobs')
profile = api.response.reply.jobProfile[-1]
if profile.get('jobStatus') == 'Created':
	print('-- job allready created')
	jid = profile.get('jobId')
	fid = profile.get('inputFileReferenceId')
	if profile.get('jobType') == job_type:
		print('-- job ok!')
		job_ok = True
	else:
		print('-- wrong jobType: %s' % profile.get('jobType'))
	print('   jobId (jid): %s - fileReferenceId (fid): %s' % (jid, fid))
else:
	api.execute('createUploadJob', job_type)
	if api.response.reply.ack == 'Success':
		print('-- job successfully created')
		jid = api.response.reply.jobId
		fid = api.response.reply.fileReferenceId
		print('   jobId: %s - fileReferenceId: %s' % (jid, fid))
		job_ok = True
	else:
		print('-- error while creating job')

if job_ok:
	bulk = BulkData(jobId=jid, fileReferenceId=fid, jobType=job_type)
	print('\n:: BulkData(%s) initialized..' % bulk.jobType)
