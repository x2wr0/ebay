from libdrebo.shop import Shop, ShopProfile
from . import Connection as Merchant
from .bulkdata import BulkData
from .accessories import EbayItem, EbaySellerList


shop = Shop(ShopProfile('ps'))
sellerlist = EbaySellerList(shop=shop, debug=True)
api=Merchant(debug=False)
