import sys
import datetime
sys.path.append('/home/brumen/work')

from rm.services.trade_api_pricers import price_trades

print(price_trades(datetime.date(2016, 7, 1), [189, 190,]))

