#! -*- coding: utf-8 -*-

# o5/2o2o: 0.2.4.7             :: wro-guys


import gzip
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
	return (ebay_strftime(now), ebay_strftime(then))

# dec = lambda s: decimal.Decimal(s)
# str2dec = lambda s: dec(s.replace(',','.'))
# dec2str = lambda d: str(d)
# QQ = dec('.01'), QP = 5
def calc_ebay_price(price, q=QQ):
	with decimal.localcontext() as context:
		context.prec = price.adjusted() + QP
		price_ebay = price * EBAY_FACTOR * TAX_DE
	return price_ebay.quantize(q)

def ebay_retrieve_ids(connection):
	"""return ebay_id, id_product"""
	with connection.cursor() as cursor:
		sql = 'SELECT id_product, ebay_id FROM ebay_items'
		cursor.execute(sql)
	return dict(cursor.fetchall())

## exemplarische Überbleibsel >>
def czekk_sellerlist(api, skus, options, cursor=None):
	result = []
	api.execute('GetSellerList', options)
	items = api.response.reply.ItemArray.Item
	sql = 'INSERT INTO ebay_items (id_product,ebay_id,reference) values (%s,%s,%s)'
	for i in items:
		sku = i.SKU
		iid = i.ItemID
		xxx = ''
		if sku in skus.keys():
			pid = skus[sku]
			if cursor:
				try:
					cursor.execute(sql, (pid, iid, sku))
				except Exception as e:
					xxx = 'ebaydb %s' % str(e)
					print('// PID: %s :: %s' % (pid, xxx))
		else:
			pid = ''
			xxx = 'keine shop-id'
			print('// SKU: %s / IID: %s :: %s' % (sku, iid, xxx))
		result.append((pid, iid, sku, xxx))
	return result

def getsellerlist(t0, t1, connection):
	# Abfrage erstellen & Übersicht anfordern
	api = Trading(warnings=True, timeout=60)
	api.execute('GetSellerList', {
		'EndTimeFrom': t0, 'EndTimeTo': t1,
		'Pagination':{'EntriesPerPage':200},
		'GranularityLevel':'Coarse',
		'OutputSelector':'PaginationResult'})
	pages = int(api.response.reply.PaginationResult.TotalNumberOfPages)
	total = int(api.response.reply.PaginationResult.TotalNumberOfEntries)
	print('   Entries: {} :: Pages: {}'.format(total, pages))

	# konkrete Abfrage
	options = {
		'EndTimeFrom':t0,
		'EndTimeTo':t1,
		'Pagination':{'EntriesPerPage':200},
		'GranularityLevel':'Coarse',
		'OutputSelector':'ItemID,SKU,PaginationResult,ReturnedItemCountActual'}

	page = 1
	result = []
	while page <= pages:
		try:
			options['Pagination']['PageNumber'] = page
			with connection.cursor() as cursor:
				result.extend(czekk_sellerlist(api, presta_skus, options, cursor))
			connection.commit()
			reply = api.response.reply
			print('-- returned item count: {} :: page: {}'.format(
				reply.ReturnedItemCountActual, page))
		#except ConnectionError as e:
		except Exception as e:
			print(e)
		page += 1
	return result
## << --------------------------

class Counter:
	def __init__(self, **kwargs):
		for key in kwargs:
			self.__dict__[key] = kwargs[key]

class EbaySellerList:
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
			'Pagination':{'EntriesPerPage': self._entries_per_page},
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
		return (self._time_from, self._time_to)
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
		#n_try = 0
		#max_tries = 3
		items = Item(ok=[], faulty=[], ids={})
		options = {
			'EndTimeFrom': self._time_from,
			'EndTimeTo': self._time_to,
			'Pagination': {'EntriesPerPage': self._entries_per_page},
			'GranularityLevel': 'Coarse',
			'OutputSelector': 'ItemID,SKU,PaginationResult,ReturnedItemCountActual'}

		def fetch_ids(options):
			ids = {}
			try:
				self._api.execute('GetSellerList', options)
				reply = self._api.response.reply
				for item in reply.ItemArray.Item:
					ids.update({item.get('ItemID'):item.get('SKU')})
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
					item = EbayItem(iid)
					item.fetch_data(cursor)
				except Exception as e:
					xxx = str(e)
					item = EbayItem(iid, sku=sku)
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
		#return items

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
								item = EbayItem(iid, pid=pid, sku=sku)
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
				shopitem = ShopItem(item.pid, self._shop)
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
		#return (items.ok, items.faulty)

class EbayItem(Item):
	""" eBay item class
	
	keyword arguments:
	item_id = eBay.ItemID *required*
	pid = product ID / shop id_product
	sku = item reference
	price = eBay.Price
	quantity = eBay.Quantity
	delivery = eBay.DispatchTimeMax
	#cursor = sql.cursor object
	"""
	def __init__(self, item_id, **kwargs):
		super(EbayItem, self).__init__()
		self.item_id = item_id
		self._price = kwargs.get('price', None)
		self._quantity = kwargs.get('quantity', None)
		self._delivery = kwargs.get('delivery', None)	# DispatchTimeMax
		self.pid = kwargs.get('pid', '')
		self.sku = kwargs.get('sku', '')
		self._data = {'ItemID': self.item_id}

	def _get_data(self):
		return self._data
	data = property(_get_data)

	def _get_reviseData(self):
		return dict2xml(self._data)
	reviseData = property(_get_reviseData)

	def _get_reviseType(self):
		if len(self._data) > 1:
			if 'DispatchTimeMax' in self._data.keys():
				return 'FixedPriceItem'
			else:
				return 'InventoryStatus'
		else:
			return None
	reviseType = property(_get_reviseType)

	def _get_delivery(self):
		return self._delivery
	def _set_delivery(self, value):
		if self._delivery != value:
			self._delivery = value
			self._data.update({'DispatchTimeMax': self._delivery})
	delivery = property(_get_delivery, _set_delivery)

	def _get_price(self):
		return self._price
	def _set_price(self, value):
		if self._price != value:
			self._price = value
			self._data.update({'StartPrice': self._price})
	price = property(_get_price, _set_price)

	def _get_quantity(self):
		return self._quantity
	def _set_quantity(self, value):
		if self._quantity != value:
			self._quantity = value
			self._data.update({'Quantity': self._quantity})
	quantity = property(_get_quantity, _set_quantity)

	def fetch_data(self, cursor):
		sql = 'SELECT id_product, reference, price, quantity, delivery FROM ebay_items WHERE ebay_id={}'
		cursor.execute(sql.format(self.item_id))
		self.pid, self.sku, self._price, self._quantity, self._delivery = cursor.fetchone()

	def update_data(self, cursor):
		"""update data in the ebay_items db"""
		sql = 'UPDATE ebay_items SET id_product, reference, price, quantity, delivery WHERE ebay_id={}'
		cursor.execute(sql.format(self.item_id))

	def insert_data(self, cursor):
		sql = 'INSERT INTO ebay_items ...'
		cursor.execute(sql.format(**values))
