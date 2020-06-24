#! -*- coding: utf-8 -*-

# o5/2o2o: 0.24.7              :: wro-guys


from datetime import datetime
from dateutil.relativedelta import relativedelta
import decimal
from time import strftime

from ebaysdk.trading import Connection as Trading

from libdrebo.shop import ShopItem
from libdrebo.utils import dec, sql_connect, Item, QQ, QP, STR_DELIVER_LATE
from libdrebo.config import sql_conf_ebay

EBAY_STR_FORMAT_TIME = '%Y-%m-%dT%H:%M:%S.000Z'
EBAY_FACTOR = dec('1.08')
TAX_DE = dec('1.19')

_sql_ebay_select = "SELECT reference, price, quantity, delivery, active, vat_percent FROM ebay_items " \
                   "WHERE id_product=%s"
_sql_ebay_update = "UPDATE ebay_items SET price=%s, quantity=%s, delivery=%s, active=%s, vat_percent=%s " \
                  "WHERE id_product=%s"
# values = pid; iid; sku; price; quantity; delivery; active; vat_percent
_sql_ebay_insert = "INSERT INTO ebay_items VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
_sql_ebay_deactivate = "UPDATE ebay_items SET quantity=0, active=0 WHERE id_product=%s"

'''
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
        self._xxx = []	# error log..
        for key in kwargs:
            self.__dict__[key] = kwargs[key]

    def _get_xxx(self):
        return ' - '.join(self._xxx)
    def _set_xxx(self, xxx):
        self._xxx.append(xxx)
    xxx = property(_get_xxx, _set_xxx)
'''

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


def ebay_retrieve_iids(connection):
    """return ebay_id, id_product"""
    with connection.cursor() as cursor:
        sql = 'SELECT ebay_id, id_product FROM ebay_items'
        cursor.execute(sql)
    return dict(cursor.fetchall())


def _item_values(item):
    prc = calc_ebay_price(item.price_retail)
    act = item.active
    if act:
        qty = item.quantity
        if qty <= 0:
            qty = 0
        elif qty > 50:
            qty = 50
    else:
        qty = 0
    if item.available == STR_DELIVER_LATE:
        dly = 15
    elif item.available == 'Lieferzeit DE 5 - 7 Tage / Lieferzeit Ausland 7 - 10 Tage':
        dly = 6
    else:
        dly = 4
    vat = 19
    return prc, qty, dly, act, vat


def _update_db(connection, shop):  # from shop
    print('\n:: update ebay_items..')
    a = u = x = 0  # a: (in)active(, i: inserted), u: updated, x: error
    items = []
    pids = ebay_retrieve_pids(connection)
    shop.fetch_pids()
    for pid in pids.keys():
        prc = qty = dly = act = vat = '---'
        iid = pids[pid]
        with connection.cursor() as cursor:
            cursor.execute(_sql_ebay_select, pid)
            sku, e_prc, e_qty, e_dly, e_act, e_vat = cursor.fetchone()
        try:
            if pid in shop.pids.keys():
                item = ShopItem(pid, shop)
                # sku = item.sku
                s_prc, s_qty, s_dly, s_act, s_vat = _item_values(item)
                if s_act and s_qty == 0:
                    s_qty = 10
                    s_dly = 15
                if not s_act:
                    a += 1
                    item.xxx = 'deactivated'
                if e_prc == s_prc or e_qty == s_qty or e_dly == s_dly or e_act == s_act or e_vat == s_vat:
                    prc = s_prc
                    qty = s_qty
                    dly = s_dly
                    act = s_act
                    vat = s_vat
                    with connection.cursor() as cursor:
                        cursor.execute(_sql_ebay_update, (prc, qty, dly, act, vat, pid))
                    u += 1
                    item.xxx = 'updated'
                xxx = item.xxx
            else:
                with connection.cursor() as cursor:
                    cursor.execute(_sql_ebay_deactivate, pid)
                a += 1
                xxx = 'not in shop, deactivated'

        except Exception as e:
            print('!! @update_db [pid: %s]: %s' % (pid, str(e)))
            x += 1
            xxx = str(e)

        items.append((pid, iid, sku, prc, qty, dly, act, vat, xxx))

    connection.commit()
    print('   %s items processed. %s updated  %s deactivated  %s error(s)' % (len(items), u, a, x))
    return items


def _update_ebay_items(connection, items):  # from ebay
    pass


class Counter:
    def __init__(self, **kwargs):
        for key in kwargs:
            self.__dict__[key] = kwargs[key]


class EbaySellerList:
    """ebay 'GetSellerList' object for a given time period"""

    def __init__(self, **kwargs):
        self._debug = kwargs.get('debug', False)
        time_from, time_to = ebay_timings(3)  # 3 months
        self.options = {'EndTimeFrom': time_from, 'EndTimeTo': time_to,
                        'Pagination': {'EntriesPerPage': 200},  # 'PageNumber': 1},
                        'GranularityLevel': 'Fine',
                        'OutputSelector': 'ItemID,SKU,Quantity,QuantitySold,StartPrice,DispatchTimeMax,'
                                          'PaginationResult,ReturnedItemCountActual,VATDetails'}
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
                data = {'ItemID': item.ItemID,
                        'SKU': item.SKU,
                        'Quantity': int(item.Quantity) - int(item.SellingStatus.QuantitySold),
                        'StartPrice': dec(item.StartPrice.value),
                        'VATPercent': dec(item.VATDetails.VATPercent),
                        'DispatchTimeMax': int(item.DispatchTimeMax)}
                items_list.append(EbayItem(**data))
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

    def fetch_items(self):
        self.fetch_items_first()  # -> page = 1
        page = 2
        options = self.options
        options['OutputSelector'] = 'ItemID,SKU,Quantity,QuantitySold,StartPrice,DispatchTimeMax,' \
                                    'VATDetails,ReturnedItemCountActual'
        while page <= self.number_pages:
            self.fetch_items_page(page, options)
            page += 1

    def update_timing(self):
        time_from, time_to = ebay_timings(3)
        self.options['EndTimeFrom'] = time_from
        self.options['EndTimeTo'] = time_to


class EbayItem(Item):
    """eBay item class"""

    def __init__(self, **kwargs):
        self._keys = ('ItemID', 'StartPrice', 'Quantity', 'DispatchTimeMax', 'SKU', 'VATPercent')
        self._data = data = {}
        for key in self._keys:
            if key in kwargs:
                data[key] = kwargs.get(key)
                del (kwargs[key])
        super(EbayItem, self).__init__(_data=data)
        self.item_id = self._data.get('ItemID', None)
        self.sku = self._data.get('SKU', None)
        self.delivery = self._data.get('DispatchTimeMax', None)
        self.quantity = self._data.get('Quantity', None)
        self.price = self._data.get('StartPrice', None)
        self.vat_percent = self._data.get('VATPercent', None)
        # self.pid = kwargs.get('pid', None)
        # self.active = kwargs.get('active', None)

    def _get_data(self):
        return self._data

    data = property(_get_data)


class EbayItemsList:
    """collection of ebay items for generating bulkdata of it"""

    def __init__(self, items=None):
        self._keys = ['FixedPriceItem', 'InventoryStatus']
        self._connection = sql_connect(sql_conf_ebay)
        self.items = Item(unsorted=[], exclude=[], **{key: [] for key in self._keys})
        self._sql_select = 'SELECT reference, price, quantity, delivery, active, vat_percent ' \
                           'FROM ebay_items WHERE ebay_id=%s'
        self._sql_insert = 'INSERT INTO ebay_items VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
        # self._pids = ebay_retrieve_pids(self._connection)
        self._iids = ebay_retrieve_iids(self._connection)
        if items:
            self.add_items(items)

    def add_item(self, item):
        data = {'ItemID': item.item_id}
        # TODO: verification of validity (item still exists in db? other issues..)
        if item.item_id in self._iids.keys():
            with self._connection.cursor() as cursor:
                cursor.execute(self._sql_select, item.item_id)
                sku, price, quantity, delivery, active, vat_percent = cursor.fetchone()

            if item.sku == sku:
                data.update({'SKU': sku})
                if item.delivery != delivery:
                    key = 'FixedPriceItem'
                    data.update({'DispatchTimeMax': delivery})
                else:
                    key = 'InventoryStatus'
                if item.quantity != quantity:
                    data.update({'Quantity': quantity})
                if item.price != price:
                    data.update({'StartPrice': price})
                if item.vat_percent != vat_percent:
                    data.update({'VATPercent': vat_percent})
                if len(data) > 1:
                    self.items.__dict__[key].append(EbayItem(pid=pid, active=active, **data))
                else:
                    item.xxx = 'nothing to put'
                    print('-- @add_item [id: %s]: %s' % (item.item_id, item.xxx))
                    print('   append to items.unsorted')
                    self.items.unsorted.append(item)
            else:
                item.xxx = 'wrong sku'
                print('!! @add_item [id: %s]: %s' % (item.item_id, item.xxx))
                print('   append to items.exclude')
                self.items.exclude.append(item)
        else:
            with self._connection.cursor() as cursor:
                cursor.execute(self._sql_insert, '...')
            item.xxx = 'inserted into db'
            print('-- @add_item [id: %s]: %s' % (item.item_id, item.xxx))

    def add_items(self, items):
        for item in items:
            try:
                self.add_item(item)
            except Exception as e:
                print('!! @add_items [id: {}]: {}'.format(item.item_id, str(e)))
                item.xxx = str(e)
                self.items.exclude.append(item)
