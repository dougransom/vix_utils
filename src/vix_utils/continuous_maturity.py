from operator import mul
from functools import partial
from .download_vix_futures import  \
pivot_futures_on_monthly_tenor,select_monthly_futures,async_load_vix_term_structure,load_vix_term_structure

import pandas as pd

from .vix_cash_term_structure import \
    async_get_vix_index_histories,  \
    pivot_cash_term_structure_on_trade_date

from .vix_futures_dates import vix_futures_settlement_date_monthly, \
    vix_futures_settlement_date_from_trade_date, \
    vix_futures_trade_dates_and_settlement_dates, \
    vix_constant_maturity_weights   



_weighted_column_names=['Open','High','Low','Close','Settle','Change']


def do_weight(trades_df,weight_df,weight_column_name,tenor):
        w=weight_df[weight_column_name]
        weight_fn=partial(mul,w)
        tenor_df=trades_df[tenor]
        v=tenor_df.apply(weight_fn)
        return v
def do_weighting_months(trades_df,weight_df,weights_and_tenors):
    return sum(do_weight(trades_df,weight_df,n,t) for n,t in weights_and_tenors )

_weights_and_tenors_vix_front_months=[('Front Month Weight',1), ('Next Month Weight',2)]

def do_weighting_front_two_months(trades_df,weight_df):
    return do_weighting_months(trades_df,weight_df,_weights_and_tenors_vix_front_months)

def continous_maturity_30day():    
    df=vix_futures_trade_dates_and_settlement_dates()
    print(f"Data frame of Trade and Settlment Dates\n{df}\ncolumns index:\n{df.columns}")
    futures_history=select_monthly_futures(load_vix_term_structure())
    futures_history_indexed_by_date=futures_history.set_index('Trade Date')
    futures_history_by_tenor=pivot_futures_on_monthly_tenor(futures_history)

    weights_all=vix_constant_maturity_weights(df)
    #we only need the weigths we for dates we have trades
    weights=weights_all[weights_all.index.isin(futures_history_indexed_by_date.index)]

    #select the front two months and the columns that have trade values
    futures_history_trade_value_columns=futures_history_by_tenor[[1,2]].swaplevel(axis=1)[_weighted_column_names].swaplevel(axis=1) 
    weighted_values=do_weighting_front_two_months(futures_history_trade_value_columns,weights)

    #just select the ones where the timestamp index is also in futures_history
    filtered_weighted_values=weighted_values[futures_history_by_tenor.index.isin(weighted_values.index)]

    return filtered_weighted_values