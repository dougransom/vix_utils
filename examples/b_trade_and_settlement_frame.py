import pandas as pd

from sample_utils import pstars
from  vix_utils   import \
    vix_futures_trade_dates_and_settlement_dates as trade_and_settle, vix_constant_maturity_weights


df=trade_and_settle()
pstars()
print(f"Data frame of Trade and Settlment Dates\n{df}\ncolumns index:\n{df.columns}")
weights=vix_constant_maturity_weights(df)
pstars()
print(f"Constant Maturity Weights{weights} column index {weights.columns}")
weights_2023=weights.loc['2023-02']
pstars()
print(f"\nConstant Maturity Weights for Feburary 2023\n{weights_2023}")
 
