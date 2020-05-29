from libdrebo.shop import Shop, ShopProfile
from .accessories import EbayItem, EbaySellerList
from . import Connection as Merchant
from . import BulkData


shop = Shop(ShopProfile('ps'))
sellerlist = EbaySellerList(shop=shop, debug=True)
api=Merchant(debug=False)
