#! -*- coding: utf-8 -*-


from time import localtime, strftime
import os

from libdrebo.shop import Shop, ShopProfile

# from .connection import Connection as Merchant
from accessories import EbaySellerList, EbayItemsList
from ebaylms.bulkdata import BulkData


PATH = os.path.join(os.path.dirname(__file__), 'stuff')

jobtypes = ['ReviseInventoryStatus', 'ReviseFixedPriceItem']

_time_local = localtime()
_str_time_local = strftime('%d.%b. %Y - %H:%M:%S', _time_local)
print(':: ebay items update :: {} ::'.format(_str_time_local))


print('\n:  create ebay sellerlist and fetch the first/all page(s)..')
sellerlist = EbaySellerList()
sellerlist.fetch_items_first()
'''
sellerlist.fetch_items_first()
for page in range(2, 6):
    sellerlist.fetch_items_page(page)
'''

shop = Shop(ShopProfile('ps'))
itemslist = EbayItemsList(shop)
itemslist.add_items(sellerlist.items)

# api = Merchant()

jt = 'ReviseFixedPriceItem'
'''
if api.jobs_created:
    for job in api.jobs_created:
        if job['jobType'] == jt:
            jid = job['jobId']
            fid = job['fileReferenceId']
else:
    api.execute('createUploadJob', jt)
    jid = api.response.reply.jobProfile.get('jobId')
    fid = api.response.reply.jobProfile.get('fileReferenceId')
'''
jid = '0123456789'
fid = '0123456789'

b = BulkData(jobId=jid, fileReferenceId=fid, jobType=jt)
b.add_items(itemslist.items.FixedPriceItem)

c = b.bder_compressed
