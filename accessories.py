#! -*- coding: utf-8 -*-

# o5/2o2o: 0.24.7              :: wro-guys


from datetime import datetime
from dateutil.relativedelta import relativedelta
import decimal
import gzip
import os
import pickle
import logging
from time import localtime, strftime

from ebaysdk.trading import Connection as Trading
from ebaysdk.parallel import Parallel

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

NAME = __name__
logger = logging.getLogger(NAME)
logger.addHandler(logging.NullHandler())


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
    with sql_connect(sql_conf_ps).cursor() as cursor:
        logger.debug('fetch brands..')
        cursor.execute('SELECT id_manufacturer, name FROM ps_manufacturer')
        manufacturers = dict(cursor.fetchall())
    logger.debug('%s brand(s) found' % len(manufacturers))

    @staticmethod
    def get_brand(mid):
        return Manufacturers.manufacturers[mid]


class Counter:
    def __init__(self, **kwargs):
        for key in kwargs:
            self.__dict__[key] = kwargs[key]


class EbaySellerList:
    """ebay 'GetSellerList' object for a given time period"""

    verb = 'GetSellerList'

    parallel = Parallel()

    _keys = ('FixedPriceItem', 'InventoryStatus')

    _filename_iids = 'item_ids.gz'

    def __init__(self):
        self.logger = logging.getLogger('%s.ItemsList' % NAME)
        self.logger.debug('instantiate ItemsList')
        self.options = None
        self.api = Trading(timeout=300)
        self._number_per_page = None
        self._number_entries = None
        self._pages = None
        self._time_from, self._time_to = None, None
        self._item_ids = set()
        self._initialized = False

    def init(self, **kwargs):
        self.logger.info('initialise %s' % EbaySellerList.verb)
        number_per_page = kwargs.get('number_per_page', 200)
        max_num_calls = kwargs.get('max_num_calls', 10)
        time_from, time_to = ebay_timings()
        options = {'EndTimeFrom': time_from, 'EndTimeTo': time_to}
        self.api.execute(EbaySellerList.verb, options)
        number_entries = int(self.api.response.reply.ReturnedItemCountActual)
        for _ in self.api.response.reply.ItemArray.Item:
            self._item_ids.add(_.ItemID)
        _ = number_entries % number_per_page
        pages = number_entries // number_per_page if _ == 0 else number_entries // number_per_page + 1
        self.logger.info('received {:d} entries total ({:d} pages)'.format(number_entries, pages))
        self.options = options
        self._time_from, self._time_to = time_from, time_to
        self._pages = pages
        self._number_per_page = number_per_page
        self._number_entries = number_entries
        self._initialized = True

    def _get_iids(self):
        return self._item_ids

    item_ids = property(_get_iids)

    def export_iids(self, filename=None, path=PATH_STUFF):
        if not filename:
            filename = EbaySellerList._filename_iids
        filename = os.path.join(path, filename)
        export_gobj(filename, self)

    @staticmethod
    def import_iids(filename=None, path=PATH_STUFF):
        if not filename:
            filename = EbaySellerList._filename_iids
        filename = os.path.join(path, filename)
        return import_gobj(filename)

    def __getstate__(self):
        state = self.__dict__.copy()
        if 'logger' in state:
            state['logger'] = state['logger'].name
        return state

    def __setstate__(self, state):
        if 'logger' in state:
            state['logger'] = logging.getLogger(state['logger'])
        self.__dict__.update(state)


class EbayItem(Item):
    """eBay item class"""

    _sql_select_iid = \
        'SELECT id_product, reference, price, quantity, delivery, active, vat_percent, ean13, mpn, brand, features ' \
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
        self.product_id = kwargs.get('id_product', None)
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


# class EbayItemsList:
#     """..."""
#     verb = 'GetSellerList'
#
#     parallel = Parallel()
#
#     _keys = ('FixedPriceItem', 'InventoryStatus')
#
#     _filename = 'itemslist.gz'
#     _filename_queue = 'itemslist.queue.gz'
#
#     def __init__(self, connection=None, catalog=None):
#         self.logger = logging.getLogger('%s.ItemsList' % NAME)
#         self.logger.debug('instantiate ItemsList')
#         self.items = Item(unsorted=[], exclude=[], **{key: [] for key in EbayItemsList._keys})
#         self.apis = None
#         self.options = None
#         self.queue = Queue()
#         self._items = []  # interim, interim, .. . .
#         self._max_num_calls = None
#         self._num_call_bursts = None
#         self._num_call_last = None
#         self._number_per_page = None
#         self._number_entries = None
#         self._pages = None
#         self._timeout = None
#         self._time_from, self._time_to = None, None
#         self._item_ids = set()
#         self._connection = connection
#         if catalog and not self._connection:
#             self._connection = EbayConnector(catalog=catalog)
#         self._initialized = False
#
#     def init(self, **kwargs):
#         self.logger.info('initialise ItemsList')
#         timeout = kwargs.get('timeout', 60)
#         number_per_page = kwargs.get('number_per_page', 200)
#         max_num_calls = kwargs.get('max_num_calls', 10)
#         time_from, time_to = ebay_timings()
#         options = {'EndTimeFrom': time_from, 'EndTimeTo': time_to}
#         api = Trading(timeout=300)
#         api.execute(EbayItemsList.verb, options)
#         number_entries = int(api.response.reply.ReturnedItemCountActual)
#         for _ in api.response.reply.ItemArray.Item:
#             self._item_ids.add(_.ItemID)
#         _ = number_entries % number_per_page
#         pages = number_entries // number_per_page if _ == 0 else number_entries // number_per_page + 1
#         self.logger.info('received {:d} entries total ({:d} pages)'.format(number_entries, pages))
#         self.apis = [Trading(timeout=timeout, parallel=EbayItemsList.parallel) for _ in range(max_num_calls)]
#         options.update({'Pagination': {'EntriesPerPage': number_per_page}, 'GranularityLevel': 'Fine'})
#         self.options = options
#         self._time_from, self._time_to = time_from, time_to
#         self._num_call_bursts = pages // max_num_calls
#         self._num_call_last = pages % max_num_calls
#         self._pages = pages
#         self._number_per_page = number_per_page
#         self._number_entries = number_entries
#         self._max_num_calls = max_num_calls
#         self._timeout = timeout
#         self._initialized = True
#
#     def _get_max_num_calls(self):
#         return self._max_num_calls
#
#     def _set_max_num_calls(self, value):
#         if self._initialized:
#             max_num_calls = int(value)
#             self.apis = [Trading(timeout=self._timeout, parallel=EbayItemsList.parallel) for _ in range(max_num_calls)]
#             self._num_call_bursts = self._pages // max_num_calls
#             self._num_call_last = self._pages % max_num_calls
#             self._max_num_calls = max_num_calls
#             self.logger.debug('\'max_num_calls\' set to %d' % self._max_num_calls)
#         else:
#             raise AttributeError('EbayItemsList not initialized')
#
#     max_num_calls = property(_get_max_num_calls, _set_max_num_calls)
#
#     def _call_burst(self, n):
#         for i, api in enumerate(self.apis):
#             page = n + i + 1
#             self.logger.debug('fetching page {:d}'.format(page))
#             self.options['Pagination']['PageNumber'] = page
#             api.execute(EbayItemsList.verb, self.options)
#
#     def _put_items(self, n):
#         for i, api in enumerate(self.apis):
#             page = n + i + 1
#             self.logger.debug('putting page {:d}'.format(page))
#             try:
#                 page = int(api.response.reply.PageNumber)
#                 if api.response.reply.Ack == 'Success':
#                     array = api.response.reply.ItemArray.Item
#                     self.queue.put(array)
#                     # with self._connection.cursor() as cursor:
#                     #    for a in array:
#                     #        item = EbayItem(a)
#                     #        if item.item_id in self._iids.keys():
#                     #            item.update(cursor)
#                     #        else:
#                     #            item.insert(cursor, self._shop, self._iids, self._pids)
#                     #        self.items.__dict__[item.key].append(item)
#                 else:
#                     self.logger.error('Ack on page {:d}: {:s}'.format(page, api.response.reply.Ack))
#             except Exception as e:
#                 self.logger.error('putting page {:d}: {:s}'.format(page, str(e)))
#
#     def fetch_items(self):
#         self.logger.info('fetching ebay items')
#         for call_burst in range(self._num_call_bursts):
#             n = call_burst * self._max_num_calls
#             self.logger.debug('call burst {:d}:'.format(call_burst+1))
#             self._call_burst(n)
#             EbayItemsList.parallel.wait()
#             error = EbayItemsList.parallel.error()
#             if error:
#                 self.logger.error('call burst {:d}: {:s}'.format(call_burst+1, error))
#                 print('retry call burst {:d}'.format(call_burst+1))
#                 self._call_burst(n)
#             else:
#                 self.logger.debug('fetched items from call burst {:d}, sorting..'.format(call_burst+1))
#             self._put_items(n)
#         if self._num_call_last:
#             self.apis = [Trading(timeout=60, parallel=EbayItemsList.parallel) for _ in range(self._num_call_last)]
#             n = self._num_call_bursts * self._max_num_calls
#             self.logger.debug('last call burst:')
#             self._call_burst(n)
#             EbayItemsList.parallel.wait()
#             error = EbayItemsList.parallel.error()
#             if error:
#                 self.logger.error('last call burst: {:s}'.format(error))
#                 print('retry last call burst')
#                 self._call_burst(n)
#             else:
#                 self.logger.debug('fetched items from last call burst, sorting..')
#             self._put_items(n)
#         print(':: finish: %s' % strftime('%d.%b. %Y - %H:%M:%S', localtime()))
#         self.logger.info('finished fetch_items')
#
#     def sort_items(self, connection=None, queue=None):
#         if not connection:
#             connection = self._connection
#         if not queue:
#             queue = self.queue
#         while queue.not_empty:
#             q = queue.get()
#             for i in q:
#                 item = EbayItem(i, connection=connection)
#                 if item.item_id in connection.iids.keys():
#                     item.update()
#                 else:
#                     item.insert()
#                 self._items.append(item)
#         connection.commit()
#
#     def export(self, filename=None, path=PATH_STUFF):
#         if not filename:
#             filename = EbayItemsList._filename
#         filename = os.path.join(path, filename)
#         export_gobj(filename, self)
#
#     def export_queue(self, filename=None, path=PATH_STUFF):
#         if not filename:
#             filename = EbayItemsList._filename_queue
#         filename = os.path.join(path, filename)
#         export_gobj(filename, self.queue)
#
#     @staticmethod
#     def import_(filename=None, path=PATH_STUFF):
#         if not filename:
#             filename = EbayItemsList._filename
#         filename = os.path.join(path, filename)
#         return import_gobj(filename)
#
#     def __getstate__(self):
#         state = self.__dict__.copy()
#         if 'logger' in state:
#             state['logger'] = state['logger'].name
#         if 'queue' in state:
#             queue = state['queue']
#             q = []
#             while not queue.empty():
#                 q.append(queue.get())
#             state['queue'] = q
#         return state
#
#     def __setstate__(self, state):
#         if 'logger' in state:
#             state['logger'] = logging.getLogger(state['logger'])
#         if 'queue' in state:
#             q = Queue()
#             for i in state['queue']:
#                 q.put(i)
#             state['queue'] = q
#         self.__dict__.update(state)
