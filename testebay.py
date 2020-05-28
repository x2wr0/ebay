from libdrebo.shop import Shop, ShopProfile
from . import EbayItem, EbaySellerList
from .merchant import Connection as Merchant
from .merchant import BulkData

dbg = False

shop = Shop(ShopProfile('ps'))
sellerlist = EbaySellerList(shop=shop, debug=dbg)
api=Merchant(debug=dbg)
