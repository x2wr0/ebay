#! -*- coding: utf-8 -*-

# o5/2o2o: 0.24.7              :: wro-guys


from datetime import datetime
from dateutil.relativedelta import relativedelta
import decimal
from time import strftime

from ebaysdk.trading import Connection as Trading
from ebaysdk.utils import dict2xml

from libdrebo.utils import dec, dec2str, str2dec, sql_connect, Item, QQ, QP, STR_DELIVER_SOON, STR_DELIVER_LATE
from libdrebo.shop import ShopItem
from libdrebo.config import sql_conf_ebay

EBAY_STR_FORMAT_TIME = '%Y-%m-%dT%H:%M:%S.000Z'
EBAY_FACTOR = dec('1.08')
TAX_DE = dec('1.19')

"""
STR_DELIVER_SOON = 'deliver soon..'
STR_DELIVER_LATE = 'deliver late..'

dec = lambda s: decimal.Decimal(s)
str2dec = lambda s: dec(s.replace(',','.'))
dec2str = lambda d: str(d)
QQ = dec('.01'), QP = 5

sql_conf_ebay = {
    'host': conf.get('host'),
    'user': conf.get('user'),
    'db': conf.get('db'),
    'password': conf.get('password'),
    'charset': conf.get('charset', fallback='utf8')}

def sql_connect(conf):
    return pymysql.connect(**conf)

class Item:
    def __init__(self, **kwargs):
        self._xxx = []	# Fehler Log
        for key in kwargs:
            self.__dict__[key] = kwargs[key]

    def _get_xxx(self):
        return ' - '.join(self._xxx)
    def _set_xxx(self, xxx):
        self._xxx.append(xxx)
    xxx = property(_get_xxx, _set_xxx)
"""

ebay_strftime = lambda t: strftime(EBAY_STR_FORMAT_TIME, t)


def ebay_timings(delta):
    now = datetime.now()
    then = now + relativedelta(months=delta)
    now = datetime.timetuple(now)
    then = datetime.timetuple(then)
    return ebay_strftime(now), ebay_strftime(then)


def calc_ebay_price(price, q=QQ):
    with decimal.localcontext() as context:
        context.prec = price.adjusted() + QP
        price_ebay = price * EBAY_FACTOR * TAX_DE
    return price_ebay.quantize(q)


def ebay_retrieve_pids(connection):
    """return ebay_id, id_product"""
    with connection.cursor() as cursor:
        sql = 'SELECT id_product, ebay_id FROM ebay_items'
        cursor.execute(sql)
    return dict(cursor.fetchall())


class Counter:
    def __init__(self, **kwargs):
        for key in kwargs:
            self.__dict__[key] = kwargs[key]


class EbaySellerList:
    """ebay 'GetSellerList' object for a given time period"""

    def __init__(self, **kwargs):
        self._debug = kwargs.get('debug', False)
        self._warnings = False
        if self._debug:
            self._warnings = True
        time_from, time_to = ebay_timings(3)  # 3 months
        self.options = {'EndTimeFrom': time_from, 'EndTimeTo': time_to,
                        'Pagination': {'EntriesPerPage': 200},  # 'PageNumber': 1},
                        'GranularityLevel': 'Fine',
                        'OutputSelector': 'ItemID,SKU,Quantity,QuantitySold,StartPrice,MaxDispatchTime'
                                          'PaginationResult,ReturnedItemCountActual,VATDetails'}
        self.items = []
        self.number_pages = 0
        self.number_entries = 0
        self.api = Trading(warnings=self._warnings, timeout=60)
        self._verb = 'GetSellerList'

    # helper method for self.fetch_items_*()
    @staticmethod
    def _append_items(items_array):
        items_list = []
        for item in items_array:
            try:
                items_list.append(EbayItem(ItemID=item.ItemID, SKU=item.SKU,
                                           Quantity=int(item.Quantity) - int(item.SellingStatus.QuantitySold),
                                           StartPrice=dec(item.StartPrice.value),
                                           VATPercent=dec(item.VATDetails.VATPercent),
                                           MaxDispatchTime=int(item.MaxDispatchTime)))
            except Exception as e:
                print('!! Error occurred [id: {}]: {}'.format(item.ItemID, str(e)))
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

    def fetch_items_page(self, page, options=None):
        if options:
            options = options
        else:
            options = self.options
        options['Pagination']['PageNumber'] = page
        self.api.execute(self._verb, options)
        if self.api.response.reply.Ack == 'Success':
            self.items.extend(self._append_items(self.api.response.reply.ItemArray.Item))

    def fetch_items(self):
        self.fetch_items_first()  # -> page = 1
        page = 2
        options = self.options
        options['OutputSelector'] = 'ItemID,SKU,Quantity,QuantitySold,StartPrice,MaxDispatchTime,VATDetails'
        while page <= self.number_pages:
            options['Pagination']['PageNumber'] = page
            self.fetch_items_page(page, options)
            page += 1

    def update_timing(self):
        time_from, time_to = ebay_timings(3)
        self.options['EndTimeFrom'] = time_from
        self.options['EndTimeTo'] = time_to


class EbayItem(Item):
    """eBay item class"""

    def __init__(self, **kwargs):
        self._keys = ('ItemID', 'StartPrice', 'Quantity', 'DispatchMaxTime', 'SKU', 'VATPercent')
        self._data = data = {}
        for key in self._keys:
            if key in kwargs:
                data[key] = kwargs.get(key)
                del (kwargs[key])
        super(EbayItem, self).__init__(_data=data, **kwargs)
        self.item_id = self._data.get('ItemID', None)
        self.sku = self._data.get('SKU', None)
        self.delivery = self._data.get('MaxDispatchTime', None)
        self.active = kwargs.get('active', None)

    def _get_data(self):
        return self._data

    data = property(_get_data)


class EbayItemsList:
    """collection of ebay items for generating bulkdata of it"""

    def __init__(self, **kwargs):
        self._keys = ['FixedPriceItem', 'InventoryStatus']
        self._connection = sql_connect(sql_conf_ebay)
        self.items = Item(unsorted=[], exclude=[], **{key: [] for key in self._keys})
        self.pids = ebay_retrieve_pids(self._connection)

    def add_item(self, item, cursor):
        data = {'ItemID': item.item_id}
        # TODO: verification of validity (item still exists in db? other issues..)
        sql = 'SELECT id_product, reference, price, quantity, delivery, active FROM ebay_items WHERE ebay_id={}'
        cursor.execute(sql.format(item.item_id))
        pid, sku, price, quantity, delivery, active = cursor.fetchone()
        if item.sku == sku:
            data.update({'SKU': sku})
            if item.delivery != delivery:
                key = 'FixedPriceItem'
                data.update({'MaxDispatchTime': delivery})
            else:
                key = 'InventoryStatus'
            if item.quantity != quantity:
                data.update({'Quantity': quantity})
            if item.price != price:
                data.update({'StartPrice': price})
            if len(data) > 1:
                self.items.__dict__[key].append(EbayItem(**data))
            else:
                self.items.unsorted.append(item)
        else:
            raise AttributeError

    def add_items(self, items):
        with self._connection.cursor() as cursor:
            for item in items:
                try:
                    self.add_item(item, cursor)
                except Exception as e:
                    print('!! Error occurred [id: {}]: {}'.format(item.item_id, str(e)))

    # should be in another place; preferably in a shop-lib / while updating the shop-db
    def update_database(self, shop):
        for pid in self.pids.keys():
            shop_item = ShopItem(pid, shop)
            if shop_item.active:
                available = shop_item.available
                delivery = shop_item.delivery
                price = calc_ebay_price(shop_item.price_retail)
                quantity = shop_item.quantity
            else:
                self.items.exclude.append(shop_item)


# ## Classics ---------------------------
'''
class EbaySellerListXXX:
    """ebay 'GetSellerList' object for a given time period

    items = {'ItemID': 'SKU'}
    """

    def __init__(self, **kwargs):
        self._entries_per_page = 200
        self.debug = kwargs.get('debug', False)
        self._time_delta = kwargs.get('tdelta', 3)
        self._time_from, self._time_to = ebay_timings(self._time_delta)
        self._skus = None
        self._shop = kwargs.get('shop', None)
        if self._shop:
            self._skus = self._shop.fetch_skus(active=True)
        self._warnings = kwargs.get('warnings', False)
        if self.debug:
            self._warnings = True
        self._timeout = kwargs.get('timeout', 60)
        print('\n:: eBay-Artikel holen..')
        self._api = Trading(warnings=self._warnings, timeout=self._timeout)
        self._api.execute('GetSellerList', {
            'EndTimeFrom': self._time_from, 'EndTimeTo': self._time_to,
            'Pagination': {'EntriesPerPage': self._entries_per_page},
            'GranularityLevel': 'Coarse',
            'OutputSelector': 'PaginationResult'})
        self._pages = int(self._api.response.reply.PaginationResult.TotalNumberOfPages)
        self._total = int(self._api.response.reply.PaginationResult.TotalNumberOfEntries)
        print('-- Entries: {} :: Pages: {}'.format(self._total, self._pages))
        self._connection = sql_connect(sql_conf_ebay)
        self._counter = Counter(n=0, r=0, x=0)
        self._items = Item(ok=[], faulty=[])
        self._items_process = {}

    def _get_counter(self):
        return self._counter

    counter = property(_get_counter)

    def _get_items(self):
        return self._items

    items = property(_get_items)

    def _get_itemslist_ok(self):
        return [[item.item_id, item.pid, item.sku, item.price, item.quantity,
                 item.delivery, item.xxx] for item in self._items.ok]

    itemslist_ok = property(_get_itemslist_ok)

    def _get_itemslist_faulty(self):
        return [[item.item_id, item.pid, item.sku, item.price, item.quantity,
                 item.delivery, item.xxx] for item in self._items.faulty]

    itemslist_faulty = property(_get_itemslist_faulty)

    def _get_time_period(self):
        return self._time_from, self._time_to

    time_period = property(_get_time_period)

    def _get_time_delta(self):
        return self._time_delta

    time_delta = property(_get_time_delta)

    def _get_items(self):
        return self._items

    items = property(_get_items)

    def _get_pages(self):
        return self._pages

    pages = property(_get_pages)

    def _get_total(self):
        return self._total

    total = property(_get_total)

    def fetch_items(self, page):
        # n_try = 0
        # max_tries = 3
        items = Item(ok=[], faulty=[], ids={})
        options = {
            'EndTimeFrom': self._time_from,
            'EndTimeTo': self._time_to,
            'Pagination': {'EntriesPerPage': self._entries_per_page},
            'GranularityLevel': 'Coarse',
            'OutputSelector': 'ItemID,SKU,Quantity,QuantitySold,PaginationResult,ReturnedItemCountActual'}

        def fetch_ids(options):
            ids = {}
            try:
                self._api.execute('GetSellerList', options)
                reply = self._api.response.reply
                for item in reply.ItemArray.Item:
                    ids.update({item.get('ItemID'): item.get('SKU')})
                if self.debug:
                    print('-- returned item count: {} :: page: {}'.format(
                        reply.ReturnedItemCountActual, page))
            except Exception as e:
                xxx = str(e)
                if self.debug:
                    print('!! {:s} [page: {}]'.format(xxx, page))
                raise e
            return ids

        options['Pagination']['PageNumber'] = page
        items.ids = fetch_ids(options)
        with self._connection.cursor() as cursor:
            for iid in items.ids.keys():
                sku = items.ids[iid]
                try:
                    item = EbayItemXXX(iid)
                    item.fetch_data(cursor)
                except Exception as e:
                    xxx = str(e)
                    item = EbayItemXXX(iid, sku=sku)
                    item.xxx = xxx
                    items.faulty.append(item)
                    print('!! {:s} [{:s}]: {:s}'.format(iid, sku, xxx))
                    self._counter.x += 1
                else:
                    if sku == item.sku:
                        items.ok.append(item)
                        self._counter.n += 1
                    else:
                        xxx = 'Fehler bei Zuordnung'
                        item.xxx = xxx
                        items.faulty.append(item)
                        if self.debug:
                            print('<< {:s} [{:s}]: {:s}'.format(iid, sku, xxx))
                        self._counter.x += 1

        self._items_process[page] = items

    # return items

    def process_items(self, page):
        items = self._items_process.pop(page)
        print(':  process items - page {:d}'.format(page))
        if len(items.faulty) > 0:
            print('   - faulty ones first..')
            n = 0
            with self._connection.cursor() as cursor:
                for item in items.faulty:
                    try:
                        sku = items.ids[item.item_id]
                        pid = self._skus[sku]
                        if item.pid == pid:
                            item.fetch_data(cursor)
                            if len(item.data) <= 1:
                                iid = item.item_id
                                item = EbayItemXXX(iid, pid=pid, sku=sku)
                                item.delivery = 0
                                item.price = 0
                                item.quantity = 0
                                item.set_data(cursor)
                            item._xxx = []
                            items.faulty.remove(item)
                            items.ok.append(item)
                            self._counter.n += 1
                            self._counter.x -= 1
                            n += 1
                    except Exception as e:
                        xxx = str(e)
                        self._items.faulty.append(item)
                        print('!! {:s} [{:s}]: {:s}'.format(item.item_id, sku, xxx))
            print('-- {:d} korrigiert'.format(n))
        print('   - the good ones..')
        for item in items.ok:
            try:
                shopitem = ShopItem(item.pid, shop=self._shop)
                delivery = 4
                if shopitem.available == STR_DELIVER_LATE:
                    delivery = 15
                quantity = shopitem.quantity
                if quantity > 50:
                    quantity = 50
                if quantity < 1:
                    quantity = 10
                    delivery = 15
                item.delivery = delivery
                item.price = calc_ebay_price(shopitem.price_retail)
                item.quantity = quantity
                rt = item.reviseType
                if rt:
                    item.xxx = rt
                    self._counter.r += 1
                    if self.debug:
                        print('>> {:s} [{:s}]: reviseType: {:s}'.format(
                            item.item_id, item.sku, rt))
                self._items.ok.append(item)
            except Exception as e:
                xxx = str(e)
                item.xxx = xxx
                self._items.faulty.append(item)
                items.ok.remove(item)
                self._counter.x += 1
                self._counter.n -= 1
                if self.debug:
                    print('!! {:s} [{:s}]: {:s}'.format(item.item_id, item.sku, xxx))
    # return (items.ok, items.faulty)
'''
