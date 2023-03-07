from operator import mul
from functools import partial
from .download_vix_futures import  \
pivot_futures_on_monthly_tenor
import pandas as pd


from .vix_futures_dates import vix_futures_settlement_date_monthly, \
    vix_futures_settlement_date_from_trade_date, \
    vix_futures_trade_dates_and_settlement_dates, \
    vix_constant_maturity_weights   



_weighted_column_names=['Open','High','Low','Close','Settle','Change']


def do_weight(trades_df:pd.DataFrame,weight_df:pd.DataFrame,weight_column_name:str,tenor:int):
    """ 
    Produces a data frame containing a scaled set of select columns.
    parameters:
    ----------
    trades_df:     a DataFrame indexed by trade date on axis 0, and by an integer (which represents a tenor which could be monthly or weekly) 
    as the first level index on axis 1.
    weight_df:  a DataFrame with columns that represent the desired weights.
    weight_column_name: the name of the column with the desired weights.
    tenor:  the column index of trades_df for the select columns to be scaled.

    """    
    w=weight_df[weight_column_name]
    weight_fn=partial(mul,w)            #a function to multiply by w
    tenor_df=trades_df[tenor]
    v=tenor_df.apply(weight_fn)         #apply to the relevant columns.
    return v

def do_weighting_months(trades_df : pd.DataFrame,weight_df:pd.DataFrame,weights_and_tenors : list[(str,int)]) ->pd.DataFrame:
    """ Produces a data frame containing the weighted mean of select columns.
    parameters:
    ----------
    trades_df:     a DataFrame indexed by trade date on axis 0, and by an integer (which represents a tenor which could be monthly or weekly) 
    as the first level index on axis 1.
    weight_df:  a dataframe with columns that represent the desired weights.
    weights_and_tenors:  a list of tuples of the name of the column weighting and the tenor to be used to find the columns in trades_df.

    """
    return sum(do_weight(trades_df,weight_df,n,t) for n,t in weights_and_tenors )

_weights_and_tenors_vix_front_months=[('Front Month Weight',1), ('Next Month Weight',2)]

def do_weighting_front_two_months(trades_df : pd.DataFrame,weight_df : pd.DataFrame) -> pd.DataFrame:
    """
    produces a weighted mean of the two nearest monthly futures resulting in an average of maturity one month.
    parameters:
    ----------
    trades_df:  a DataFrame indexed by trade date on axis 0, and by MonthTenor (as the first level index) on axis 1.

    """
    return do_weighting_months(trades_df,weight_df,_weights_and_tenors_vix_front_months)

def continuous_maturity_30day(monthly_records : pd.DataFrame)->pd.DataFrame:   
    """
    produces a weighted mean of the two nearest monthly futures resulting in an average of maturity one month.
    parameters
    ----------
    monthly_records:
        A DataFrame in a record format with monthly settlements only, cotainging the vix futures history. 
        produced by vix_futures_trade_dates_and_settlement_dates or async_vix_futures_trade_dates_and_settlement_dates
    """
    
    df=vix_futures_trade_dates_and_settlement_dates()

    #bail if caller included weekly settlements
    weekly=monthly_records[monthly_records['Weekly'] == True]
    assert weekly.shape[0]==0,  "monthly_records must not contain weeklies, filter with select_monthly_futures(...)"

    futures_history=pd.DataFrame(monthly_records)
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