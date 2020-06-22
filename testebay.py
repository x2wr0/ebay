#! -*- coding: utf-8 -*-

from libdrebo.shop import Shop, ShopProfile, ShopItem
from libdrebo.utils import sql_connect, STR_DELIVER_LATE
from libdrebo.config import sql_conf_ebay

from . import Connection as Merchant
from .accessories import EbaySellerList, EbayItemsList, ebay_retrieve_iids, ebay_retrieve_pids, calc_ebay_price
from .bulkdata import BulkData


_job_ok = False
jobtype = 'ReviseInventoryStatus'
# jobtype = 'ReviseFixedPriceItem'

_sql_ebay_select = "SELECT price, quantity, delivery, active, vat_percent FROM ebay_items WHERE id_product=%s"
_sql_ebay_update = "UPDATE ebay_items SET price=%s, quantity=%s, delivery=%s, active=%s, vat_percent=%s " \
                  "WHERE id_product=%s"
_sql_ebay_insert = "INSERT INTO ebay_items VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
_sql_ebay_deactivate = "UPDATE ebay_items SET quantity=0, active=0 WHERE id_product=%s"


def _item_values(item):
    prc = calc_ebay_price(item.price_retail)
    qty = item.quantity
    if qty < 0:
        qty = 0
    elif qty > 50:
        qty = 50
    if item.available == STR_DELIVER_LATE:
        dly = 15
    elif item.available == 'Lieferzeit DE 5 - 7 Tage / Lieferzeit Ausland 7 - 10 Tage':
        dly = 6
    else:
        dly = 4
    act = item.active
    vat = 19
    return prc, qty, dly, act, vat


def _update_ebay_db(items, db):
    n = len(items)
    a = i = u = x = 0
    for item in items:
        iid = item.item_id
        pid = ''
        if iid in _ebay_iids.keys():
            try:
                pid = _ebay_iids[iid]
                shop_item = ShopItem(pid, _shop)
                if shop_item.active:
                    price, quantity, delivery, active, vat = _item_values(shop_item)
                    try:
                        with db.cursor() as cursor:
                            cursor.execute(_sql_ebay_update, (price, quantity, delivery, active, vat, pid))
                            u += 1
                    except Exception as e:
                        print('!! @update [pid: %s, iid: %s]: %s' % (pid, iid, str(e)))
                        print('   sku: %s  prc: %s  qty: %s  dly: %s  act: %s  vat: %s' % (
                            item.sku, price, quantity, delivery, active, vat))
                        x += 1
                else:
                    print('-- set item [pid: %s, iid: %s] inactive' % (pid, iid))
                    with db.cursor() as cursor:
                        cursor.execute(_sql_ebay_deactivate, pid)
                        a += 1
            except Exception as e:
                print('!! @update [iid: %s]: %s' % (iid, str(e)))
        else:
            try:
                sku = item.sku
                pid = _shop_skus[sku]
                if sku in _shop_skus.keys():
                    shop_item = ShopItem(pid, _shop)
                    if shop_item.active:
                        price, quantity, delivery, active, vat = _item_values(shop_item)
                        with db.cursor() as cursor:
                            cursor.execute(_sql_ebay_insert, (pid, iid, sku, price, quantity, delivery, active, vat))
                            i += 1
                    else:
                        print('-- set item [pid: %s, iid: %s] inactive' % (pid, iid))
                        with db.cursor() as cursor:
                            cursor.execute(_sql_ebay_deactivate, pid)
                            a += 1
                else:
                    print('-- set item [pid: %s, iid: %s] inactive: [sku: %s] not valid' % (pid, iid, sku))
                    with db.cursor() as cursor:
                        cursor.execute(_sql_ebay_deactivate, pid)
                        a += 1
            except Exception as e:
                print('!! @insert [pid: %s, iid: %s]: %s' % (pid, iid, str(e)))
                x += 1
    print('-- %s items processed: %s updated, %s inserted, %s deactivated, %s errors' % (n, u, i, a, x))
    db.commit()


print('\n:: validate job %s..' % jobtype)
api = Merchant(debug=False)
api.execute('getJobs')
_profile = api.response.reply.jobProfile[-1]
_jid = _fid = ''
if _profile.get('jobStatus') == 'Created':
    print('-- job already created')
    _jid = _profile.get('jobId')
    _fid = _profile.get('inputFileReferenceId')
    if _profile.get('jobType') == jobtype:
        print('-- jobType ok')
        _job_ok = True
    else:
        print('-- wrong jobType: %s' % _profile.get('jobType'))
    print('   jobId (jid): %s  fileReferenceId (fid): %s' % (_jid, _fid))
else:
    api.execute('createUploadJob', jobtype)
    if api.response.reply.ack == 'Success':
        print('-- job created successfully')
        _jid = _profile.get('jobId')
        _fid = _profile.get('inputFileReferenceId')
        print('   jobId (jid): %s  fileReferenceId (fid): %s' % (_jid, _fid))
        _job_ok = True
    else:
        print('!! error occurred while job creation - abort!')


if _job_ok:
    print('\n:: create ebay sellerlist and fetch the first/all page(s)..')
    sellerlist = EbaySellerList()
    sellerlist.fetch_items()
#    sellerlist.fetch_items_first()
#    for page in range(2, 6):
#        sellerlist.fetch_items_page(page)

    print('\n:: connect shops..')
    _ebay_db = sql_connect(sql_conf_ebay)
    _ebay_pids = ebay_retrieve_pids(_ebay_db)
    _ebay_iids = ebay_retrieve_iids(_ebay_db)
    _shop = Shop(ShopProfile('ps'))
    _shop_pids = _shop.fetch_pids()
    _shop_skus = _shop.fetch_skus()

    print('\n:: update ebay_db..')
    _update_ebay_db(sellerlist.items, _ebay_db)

    print('\n:: create ebay itemslist..')
    itemslist = EbayItemsList(sellerlist.items)
    _n = len(itemslist.items.InventoryStatus)
    _m = len(itemslist.items.FixedPriceItem)
    print('   %s item(s) in InventoryStatus  %s items(s) in FixedPriceItem' % (_n, _m))

    print('\n:: initialize bulkdata(%s)..' % jobtype)
    bulkdata = BulkData(jobId=_jid, fileReferenceId=_fid, jobType=jobtype)
