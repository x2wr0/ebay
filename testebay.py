from .libdrebo.shop import Shop, ShopProfile
from .libdrebo.ebay import EbayItem, EbaySellerList
from .libdrebo.ebay.merchant import Connection as Merchant
from .libdrebo.ebay.merchant import BulkData

shop = Shop(ShopProfile('ps'))
sellerlist = EbaySellerList(shop=shop, debug=True)
api=Merchant(debug=True)
