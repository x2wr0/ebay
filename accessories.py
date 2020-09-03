#! -*- coding: utf-8 -*-

# o5/2o2o: 0.24.7              :: wro-guys


from datetime import datetime
from dateutil.relativedelta import relativedelta
import decimal
import gzip
import os
import pickle
from time import strftime

from ebaysdk.trading import Connection as Trading

from libdrebo.shop import ShopItem
from libdrebo.utils import dec, sql_connect, Item, QQ, QP, STR_DELIVER_LATE, csv_write
from libdrebo.config import sql_conf_ebay, sql_conf_ps

VAT = 16.0
TAX_DE = dec('1.16')
EBAY_FACTOR = dec('1.08') * TAX_DE
EBAY_STR_FORMAT_TIME = '%Y-%m-%dT%H:%M:%S.000Z'

PATH = os.path.dirname(__file__)
PATH_STUFF = os.path.join(PATH, 'stuff')

ebay_strftime = lambda t: strftime(EBAY_STR_FORMAT_TIME, t)


def gpickle(obj):
    return gzip.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL))


def gunpickle(gobj):
    return pickle.loads(gzip.decompress(gobj))


def export_gobj(filename, obj):
    fn = os.path.join(PATH_STUFF, filename)
    with open(fn, 'wb') as f:
        o = gpickle(obj)
        f.write(o)


def import_gobj(filename):
    fn = os.path.join(PATH_STUFF, filename)
    with open(fn, 'rb') as f:
        o = f.read()
        return gunpickle(o)


def ebay_timings(delta=3):
    now = datetime.now() + relativedelta(days=-1)
    then = now + relativedelta(months=delta)
    now = datetime.timetuple(now)
    then = datetime.timetuple(then)
    return ebay_strftime(now), ebay_strftime(then)


def calc_ebay_price(price, q=QQ):
    with decimal.localcontext() as context:
        context.prec = price.adjusted() + QP
        price_ebay = price * EBAY_FACTOR
    return price_ebay.quantize(q)


def ebay_retrieve_pids(connection):
    """return ebay_id, id_product"""
    with connection.cursor() as cursor:
        cursor.execute('SELECT id_product, ebay_id FROM ebay_items')
    return dict(cursor.fetchall())


def ebay_retrieve_iids(connection):
    """return ebay_id, id_product"""
    with connection.cursor() as cursor:
        cursor.execute('SELECT ebay_id, id_product FROM ebay_items')
    return dict(cursor.fetchall())


def get_ebay_items():  # just for fun..!
    items = []
    time_from, time_to = ebay_timings()
    options = {'EndTimeFrom': time_from, 'EndTimeTo': time_to}
    api = Trading(timeout=300)
    api.execute('GetSellerList', options)
    for item in api.response.reply.ItemArray.Item:
        items.append(EbayItem(ItemID=item.ItemID))
    print('   %d items returned' % len(items))
    return items


class Manufacturers:

    _sql = 'SELECT id_manufacturer, name FROM ps_manufacturer'

    connection = sql_connect(sql_conf_ps)
    with connection.cursor() as cursor:
        print(':  fetch brands..')
        cursor.execute(_sql)
        manufacturers = dict(cursor.fetchall())
    connection.close()
    print('   %s brand(s) found' % len(manufacturers))

    @staticmethod
    def get_brand(mid):
        return Manufacturers.manufacturers[mid]


class Counter:
    def __init__(self, **kwargs):
        for key in kwargs:
            self.__dict__[key] = kwargs[key]


class EbaySellerList:
    """ebay 'GetSellerList' object for a given time period"""

    def __init__(self, **kwargs):
        self._debug = kwargs.get('debug', False)
        self.time_from, self.time_to = ebay_timings()
        self.options = {'EndTimeFrom': self.time_from, 'EndTimeTo': self.time_to,
                        'Pagination': {'EntriesPerPage': 200},  # 'PageNumber': 1},
                        'GranularityLevel': 'Fine'}  # ,
#                        'OutputSelector': 'ItemID,SKU,Quantity,QuantitySold,StartPrice,DispatchTimeMax,'
#                                          'PaginationResult,ReturnedItemCountActual,VATPercent,BrandMPN,EAN'}
        if self._debug:
            self.options.update({'WarningLevel': 'High'})
        self.items = []
        self.number_pages = 0
        self.number_entries = 0
        self.api = Trading(warnings=self._debug, timeout=60)
        self._verb = 'GetSellerList'

    # helper method for self.fetch_items_*()
    @staticmethod
    def _append_items(items_array):
        items_list = []
        for item in items_array:
            try:
                items_list.append(EbayItem(item))
            except Exception as e:
                print('!! @_append_items [id: %s]: %s' % (item.ItemID, str(e)))
        return items_list

    def fetch_items_first(self, options=None):
        if options:
            options = options
        else:
            options = self.options
        self.items = []
        self.api.execute(self._verb, options)
        if self.api.response.reply.Ack == 'Success':
            self.number_entries = int(self.api.response.reply.PaginationResult.TotalNumberOfEntries)
            self.number_pages = int(self.api.response.reply.PaginationResult.TotalNumberOfPages)
            self.items.extend(self._append_items(self.api.response.reply.ItemArray.Item))
            print('-- entries: {}  pages: {}'.format(self.number_entries, self.number_pages))
            print('   page: 1  entries: {}'.format(self.api.response.reply.ReturnedItemCountActual))

    def fetch_items_page(self, page, options=None):
        if options:
            options = options
        else:
            options = self.options
        options['Pagination']['PageNumber'] = page
        self.api.execute(self._verb, options)
        if self.api.response.reply.Ack == 'Success':
            self.items.extend(self._append_items(self.api.response.reply.ItemArray.Item))
            print('   page: {}  entries: {}'.format(page, self.api.response.reply.ReturnedItemCountActual))
            number_pages = int(self.api.response.reply.PaginationResult.TotalNumberOfPages)
            if number_pages != self.number_pages:
                self.number_pages = number_pages
                self.number_entries = int(self.api.response.reply.PaginationResult.TotalNumberOfEntries)
                print('-- {} entries on {} pages'.format(self.number_entries, self.number_pages))

    def fetch_items(self, start_page=None):
        if start_page:
            self.number_pages = page = start_page
        else:
            self.fetch_items_first()  # -> page = 1
            page = 2
        # options = self.options
        # options['OutputSelector'] = 'ItemID,SKU,Quantity,QuantitySold,StartPrice,DispatchTimeMax,' \
        #                            'VATDetails,MPN,EAN,ReturnedItemCountActual'
        # TODO: error handling (e.g. "page number out of range.")
        while page <= self.number_pages:
            try:
                self.fetch_items_page(page)  # , options)
            except ConnectionResetError:
                print('!! connection lost, reconnecting')
                self.api = Trading(warnings=self._debug, timeout=60)
                self.fetch_items_page(page)  # , options)
            finally:
                page += 1

    def update_timing(self, months=3):
        self.time_from, self.time_to = ebay_timings(months)
        self.options['EndTimeFrom'] = self.time_from
        self.options['EndTimeTo'] = self.time_to

    def export_obj(self):  # for debugging
        filename = 'sellerlist.items.gz'
        export_gobj(filename, self.items)

    @staticmethod
    def import_gobj():
        filename = 'sellerlist.items.gz'
        return import_gobj(filename)


class EbayItem(Item):
    """eBay item class"""

    _sql_select_iid = \
        'SELECT id_product, reference, price, quantity, delivery, active, vat_percent, ean13, mpn, mid, features ' \
        'FROM ebay_items WHERE ebay_id=%s'

    @staticmethod
    def compile_specifics(features):
        specifics = {}
        for key in features:
            if features[key]:
                if len(key) <= 65 and len(features[key]) <= 65:
                    specifics.update({key: features[key]})
        return specifics

    def __init__(self, item=None, **kwargs):
        if item:
            self.item_id = item.ItemID
            self.sku = item.SKU
            self.quantity = int(item.Quantity) - int(item.SellingStatus.QuantitySold)
            self.price = dec(item.StartPrice.value)
            self.delivery = int(item.DispatchTimeMax)
            self.vat_percent = float(item.VATDetails.VATPercent)
            self.ean13 = None  # item.ProductListingDetails.EAN
            self.mpn = None  # item.ProductListingDetails.BrandMPN.MPN
            self.brand = None  # item.ProductListingDetails.BrandMPN.Brand
            self.category_id_ebay = item.PrimaryCategory.CategoryID
            self.category_id_store = item.Storefront.StoreCategoryID
            self.active = True
        else:
            self.item_id = kwargs.get('ItemID', None)
            self.sku = kwargs.get('SKU', None)
            self.quantity = kwargs.get('Quantity', None)
            self.price = kwargs.get('StartPrice', None)
            self.delivery = kwargs.get('DispatchTimeMax', None)
            self.vat_percent = kwargs.get('VATPercent', None)
            self.ean13 = kwargs.get('EAN', None)
            self.mpn = kwargs.get('MPN', None)
            self.brand = kwargs.get('Brand', None)
            self.category_id_ebay = kwargs.get('CategoryID', None)
            self.category_id_store = kwargs.get('StoreCategoryID', None)
            self.active = kwargs.get('active', True)
        self.product_id = kwargs.get('product_id', None)
        self._mid = kwargs.get('mid', None)  # manufacturer id
        self._key = kwargs.get('key', None)
        self.features = kwargs.get('features', None)
        self.specifics = kwargs.get('specifics', None)
        self.exclude = False
        data = {'ItemID': self.item_id, 'SKU': self.sku}
        super(EbayItem, self).__init__(_data=data)

    def update_data(self, cursor):
        data = {}
        xxx = set()
        cursor.execute(EbayItem._sql_select_iid, self.item_id)
        product_id, sku, price, quantity, delivery, active, vat_percent, ean13, mpn, mid, features = cursor.fetchone()
        self.product_id = product_id
        u = 0
        if active:
            brand_mpn = {}
            x = set()
            if self.sku != sku:
                self.sku = sku
                data.update({'SKU': self.sku})
                x.add('sku')
                u += 1
            if self.price != price:
                self.price = price
                data.update({'StartPrice': self.price})
                x.add('price')
                u += 1
            if self.quantity != quantity:
                self.quantity = quantity
                data.update({'Quantity': self.quantity})
                x.add('quantity')
                u += 1
            if self.delivery != delivery:
                self.delivery = delivery
                data.update({'DispatchTimeMax': self.delivery})
                x.add('delivery')
                u += 1
            if self.vat_percent != vat_percent:
                self.vat_percent = vat_percent
                data.update({'VATDetails': {'VATPercent': self.vat_percent}})
                x.add('vat')
                u += 1
            if self._mid != mid:
                self._mid = mid
                if self._mid == 0:
                    self.brand = 'Markenlos'
                else:
                    self.brand = Manufacturers.get_brand(self._mid)
                brand_mpn.update({'Brand': self.brand})
                # x.add('brand')
            self.ean13 = ean13
            self.mpn = mpn
            brand_mpn.update({'MPN': self.mpn})
            data.update({'ProductListingDetails': {'EAN': self.ean13, 'BrandMPN': brand_mpn}})
            xml = ''
            if features:
                self.features = gunpickle(features)
                self.specifics = EbayItem.compile_specifics(self.features)
                for key in self.specifics.keys():
                    s = '<NameValueList><Name>{}</Name><Value>{}</Value></NameValueList>\n'
                    xml += s.format(key, self.specifics[key])
            xml += '<NameValueList><Name>Marke</Name><Value>{}</Value></NameValueList>\n'.format(self.brand)
            xml += '<NameValueList><Name>MPN</Name><Value>{}</Value></NameValueList>\n'.format(self.mpn)
            data.update({'ItemSpecifics': xml})

            if len(x) > 0:
                s = '/'.join(x)
                xxx.add('updated %s' % s)
        else:
            self.exclude = True
            self.active = False
            self.quantity = 0
            self._key = 'exclude'
            data.update({'Quantity': self.quantity})
            xxx.add('inactive')

        if len(xxx) > 0:
            xxx = ' - '.join(xxx)
            self._xxx.append(xxx)
        # if len(data) > 1 and not self.exclude:
        if u > 0 and not self.exclude:
            # right now, for us, there is only 'FixedPriceItem'.. sadly.. . .
            self._key = 'FixedPriceItem'
            '''
            if self.delivery != delivery or self.vat_percent != vat_percent or ean13:
                self._key = 'FixedPriceItem'
            else:
                self._key = 'InventoryStatus'
            self._data.update(data)
            '''
        else:
            self._key = 'unsorted'
            self._xxx.append('nothing to update')
        self._data.update(data)

    def _get_data(self):
        return self._data

    data = property(_get_data)

    def _get_key(self):
        return self._key

    key = property(_get_key)


class EbayItemsList:
    """collection of ebay items for generating bulkdata of it"""

    _sql_select_pid = 'SELECT reference, price, quantity, delivery, active, vat_percent, ean13, mpn, mid, features ' \
                      'FROM ebay_items WHERE id_product=%s'
    _sql_update_pid = 'UPDATE ebay_items SET reference=%s, price=%s, quantity=%s, delivery=%s, active=%s, ' \
                      'vat_percent=%s, ean13=%s, mpn=%s, mid=%s, features=%s WHERE id_product=%s'
    # _sql_update_pid = 'UPDATE ebay_items SET {values} WHERE id_product=%s'  # for dynamically generated list of values
    _sql_deactivate_iid = 'UPDATE ebay_items SET quantity=0, active=0 WHERE ebay_id=%s'
    _sql_deactivate_pid = 'UPDATE ebay_items SET quantity=0, active=0 WHERE id_product=%s'
    _sql_insert = 'INSERT INTO ebay_items ' \
                  '(id_product, ebay_id, reference, price, quantity, delivery, active, vat_percent, ' \
                  'ean13, mpn, mid, features) ' \
                  'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    def __init__(self, shop=None):
        self._keys = ['FixedPriceItem', 'InventoryStatus']
        self._connection = sql_connect(sql_conf_ebay)
        self.items = Item(unsorted=[], exclude=[], **{key: [] for key in self._keys})
        # self._pids = ebay_retrieve_pids(self._connection)
        self._item_ids = ebay_retrieve_iids(self._connection)
        if shop:
            if not shop.skus:
                shop.fetch_skus(active=True)
            if not shop.pids:
                shop.fetch_pids(active=True)
        self._shop = shop

    def add_item(self, item):
        if item.item_id in self._item_ids.keys():
            with self._connection.cursor() as cursor:
                item.update_data(cursor)
        else:
            sku = item.sku
            if sku in self._shop.skus.keys():
                product_id = self._shop.skus[sku]
                self._shop.ping()
                shopitem = ShopItem(product_id, self._shop)
                price = calc_ebay_price(shopitem.price_retail)
                quantity = shopitem.quantity
                if quantity <= 0:
                    quantity = 10
                elif quantity > 50:
                    quantity = 50
                if shopitem.available == STR_DELIVER_LATE:
                    delivery = 15
                elif shopitem.available == 'Lieferzeit DE 5 - 7 Tage / Lieferzeit Ausland 7 - 10 Tage':
                    delivery = 6
                else:
                    delivery = 4
                vat_percent = VAT
                if shopitem.ean13:
                    ean = shopitem.ean13
                else:
                    ean = 'Nicht zutreffend'
                if shopitem.mpn:
                    mpn = shopitem.mpn
                else:
                    mpn = 'Nicht zutreffend'
                mid = shopitem.manufacturerid
                fts = gpickle(self._shop.fetch_features_ps(shopitem))
                if product_id in self._item_ids.values():
                    active = True
                    values = (item.item_id, price, quantity, delivery, active, vat_percent,
                              ean, mpn, mid, fts, product_id)
                    sql_update = 'UPDATE ebay_items SET ebay_id=%s, price=%s, quantity=%s, delivery=%s, active=%s, ' \
                                 'vat_percent=%s, ean13=%s, mpn=%s, mid=%s, features=%s WHERE id_product=%s'
                    with self._connection.cursor() as cursor:
                        cursor.execute('SELECT ebay_id FROM ebay_items WHERE id_product=$s', product_id)
                        item_id = cursor.fetchone()[0]
                        del self._item_ids[item_id]
                        cursor.execute(sql_update, values)
                        item.update_data(cursor)
                    item.xxx = 'updated with new ebay item id'
                else:
                    values = (product_id, item.item_id, sku, price, quantity, delivery,
                              item.active, vat_percent, ean, mpn, mid, fts)
                    with self._connection.cursor() as cursor:
                        cursor.execute(EbayItemsList._sql_insert, values)
                        item.update_data(cursor)
                    item.xxx = 'inserted into db'
                self._item_ids.update({item.item_id: product_id})
            else:
                with self._connection.cursor() as cursor:
                    cursor.execute(EbayItemsList._sql_deactivate_iid, item.item_id)
                    item.update_data(cursor)
                item.xxx = 'not in shop (inactive)'
                # item.exclude = True

        if item.exclude:
            print('!! @add_item [id: %s]: %s' % (item.item_id, item.xxx))
        self.items.__dict__[item.key].append(item)

    def add_items(self, items):
        print(':  adding items..')
        for item in items:
            try:
                self.add_item(item)
            except Exception as e:
                item.xxx = str(e)
                item.exclude = True
                self.items.exclude.append(item)
                print('!! @add_items [iid: %s]: %s' % (item.item_id, item.xxx))
        self.commit()
        print('  %s items processed. %s sorted, %s unsorted, %s excluded' % (
            len(items), len(self.items.FixedPriceItem)+len(self.items.InventoryStatus),
            len(self.items.unsorted), len(self.items.exclude)))

    def update_db(self):
        a = u = x = 0  # a: (in)active[, i: inserted], u: updated, x: error
        ebay_pids = ebay_retrieve_pids(self._connection)
        print(':  update db..')
        for pid in ebay_pids.keys():
            iid = ebay_pids[pid]
            values2update = {}
            with self._connection.cursor() as cursor:
                cursor.execute(EbayItemsList._sql_select_pid, pid)
            e_sku, e_prc, e_qty, e_dly, e_act, e_vat, e_ean, e_mpn, e_mid, e_fts = cursor.fetchone()
            if pid in self._shop.pids.keys():
                s_sku = self._shop.pids[pid]
                try:
                    shopitem = ShopItem(pid, self._shop)
                    s_fts = self._shop.fetch_features_ps(shopitem)
                    if s_fts:
                        s_fts = gpickle(s_fts)
                    values2update.update({'features': s_fts})

                    s_prc = calc_ebay_price(shopitem.price_retail)
                    s_qty = shopitem.quantity
                    if s_qty <= 0:
                        s_qty = 10
                    elif s_qty > 50:
                        s_qty = 50

                    d = shopitem.available
                    if d == STR_DELIVER_LATE:
                        s_dly = 15
                    elif d == 'Lieferzeit DE 5 - 7 Tage / Lieferzeit Ausland 7 - 10 Tage':
                        s_dly = 6
                    else:
                        s_dly = 4

                    ean = shopitem.ean13
                    if not ean:
                        s_ean = 'Nicht zutreffend'
                    else:
                        s_ean = ean

                    mpn = shopitem.mpn
                    if not mpn:
                        s_mpn = 'Nicht zutreffend'
                    else:
                        s_mpn = mpn

                    s_vat = VAT
                    s_mid = shopitem.manufacturerid
                    s_act = True

                    if s_sku != e_sku:
                        values2update.update({'reference': s_sku})
                    if s_prc != e_prc:
                        values2update.update({'price': s_prc})
                    if s_qty != e_qty:
                        values2update.update({'quantity': s_qty})
                    if s_dly != e_dly:
                        values2update.update({'delivery': s_dly})
                    if s_vat != e_vat:
                        values2update.update({'vat_percent': s_vat})
                    if s_ean != e_ean:
                        values2update.update({'ean13': s_ean})
                    if s_mpn != e_mpn:
                        values2update.update({'mpn': s_mpn})
                    if s_mid != e_mid:
                        values2update.update({'mid': s_mid})
                    if s_act != e_act:
                        values2update.update({'active': s_act})
                    values = (s_sku, s_prc, s_qty, s_dly, s_act, s_vat, s_ean, s_mpn, s_mid, s_fts, pid)
                    if len(values2update) > 0:
                        # TODO: sql query should be more dynamic (values list..)!
                        with self._connection.cursor() as cursor:
                            cursor.execute(EbayItemsList._sql_update_pid, values)
                        u += 1
                except Exception as e:
                    print('!! @update_db [pid: %s, iid: %s]: %s' % (pid, iid, str(e)))
                    x += 1
            else:
                with self._connection.cursor() as cursor:
                    cursor.execute(EbayItemsList._sql_deactivate_pid, pid)
                print('!! @update_db [pid: %s, iid: %s]: not in shop (inactive), deactivated' % (pid, iid))
                a += 1

        self.commit()
        print('   %s items processed. %s updated, %s deactivated, %s error(s)' % (len(ebay_pids.keys()), u, a, x))

    def commit(self):
        self._connection.commit()

    def export_csv(self, key):  # for debugging
        content = []
        for item in self.items.__dict__[key]:
            content.append((item.product_id, item.item_id, item.sku, item.price, item.quantity, item.delivery,
                            item.active, item.vat_percent, item.ean13, item.mpn, item.xxx))
        filename = os.path.join(PATH_STUFF, 'itemslist.{}.csv'.format(key))
        csv_write(filename, content, fieldnames=[
            'pid', 'iid', 'sku', 'prc', 'qty', 'dly', 'act', 'vat', 'ean', 'mpn', 'xxx'])

    def export_obj(self, key=None):  # for debugging
        if key:
            filename = 'itemslist.{}.items.gz'.format(key)
            export_gobj(filename, self.items.__dict__[key])
        else:
            filename = 'itemslist.items.gz'
            export_gobj(filename, self.items)

    @staticmethod
    def import_gobj(key=None):
        if key:
            filename = 'itemslist.{}.items.gz'.format(key)
        else:
            filename = 'itemslist.items.gz'
        return import_gobj(filename)
