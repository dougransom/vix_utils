import pandas as pd

from  vix_utils.vix_futures_term_structure  import vix_futures_trade_dates_and_settlement_dates as trade_and_settle


df=trade_and_settle()

print(f"Data frame of Trade and Settlmeent Dates\n{df}\ncolumns index:\n{df.columns}")

