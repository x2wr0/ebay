from libdrebo.shop import Shop, ShopProfile
from . import EbayItem, EbaySellerList
from .merchant import Connection as Merchant
from .merchant import BulkData

shop = Shop(ShopProfile('ps'))
sellerlist = EbaySellerList(shop=shop, debug=True)
api=Merchant(debug=True)
