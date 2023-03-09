import pandas as pd
from functools import partial,reduce
from operator import add,mul
from sample_utils import pstars
from  vix_utils   import \
    vix_futures_trade_dates_and_expiry_dates as trade_and_settle, vix_constant_maturity_weights, load_vix_term_structure,\
    select_monthly_futures,pivot_futures_on_monthly_tenor


df=trade_and_settle()
pstars()
print(f"Data frame of Trade and Settlment Dates\n{df}\ncolumns index:\n{df.columns}")
weights=vix_constant_maturity_weights(df)
pstars()
print(f"Constant Maturity Weights{weights} column index {weights.columns}")
weights_2023=weights.loc['2023-02']
pstars()
print(f"\nConstant Maturity Weights for Feburary 2023\n{weights_2023}")
 
futures_history=select_monthly_futures(load_vix_term_structure())
pstars()
print(f"\nPivoting")
futures_history_by_tenor=pivot_futures_on_monthly_tenor(futures_history)
#the security values that we can combine to make a constant maturity 
#  
_weighted_column_names=['Open','High','Low','Close','Settle','Change']


#select the front two months and the columns that have trade values
futures_history_trade_value_columns=futures_history_by_tenor[[1,2]].swaplevel(axis=1)[_weighted_column_names].swaplevel(axis=1) 

print(f"\nfutures_history_trade_value_columns:\n{futures_history_trade_value_columns}")
pstars()

#can't multiply a dataframe by a series, we have to do it by column
weighted_values=pd.DataFrame()

def do_weight(weight_column_name,tenor):
        w=weights[weight_column_name]
        weight_fn=partial(mul,w)
        tenor_df=futures_history_trade_value_columns[tenor]
        v=tenor_df.apply(weight_fn)
        return v



      

#weighted_values=reduce(add,do_weighting())
 
w=weighted_values 
fcv=futures_history_trade_value_columns
 