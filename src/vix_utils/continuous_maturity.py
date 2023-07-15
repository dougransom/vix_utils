from operator import mul
from functools import partial
import numpy as np
from .download_vix_futures import  \
pivot_futures_on_monthly_tenor
import pandas as pd
import logging

from .vix_futures_dates import vix_futures_expiry_date_monthly, \
    vix_futures_expiry_date_from_trade_date, \
    vix_futures_trade_dates_and_expiry_dates, \
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
    trades_df:  a DataFrame indexed by trade date on axis 0, and by Tenor_Monthly (as the first level index) on axis 1.

    """
    return do_weighting_months(trades_df,weight_df,_weights_and_tenors_vix_front_months)

def append_continuous_maturity_one_month(monthly_wide_records : pd.DataFrame)->pd.DataFrame:
    """
    produces a weighted mean of the two nearest monthly futures (using continous_maturity_30day)
    appends it to the monthly_wide_records, with Monthly_Tenor of 1.5 (for ease of sorting)
    There will be fewer columns as the result of continous_maturity_30day has fewer columns for a tenor than 
    monthly_wide_records.
    parameters:
    -----------
    monthly_records:
        A DataFrame in a wide format of Monthly records, as returned by  pivot_futures_on_monthly_tenor     
   
    """

    cm=continuous_maturity_one_month(monthly_wide_records)
    wide_columns=monthly_wide_records[1].columns
    #intersection of columns in the two data frames
     
    cols=[col for col in cm.columns if col in wide_columns ]
    new_df1=monthly_wide_records.swaplevel(axis=1)[cols]
    new_cm=cm[cols]
    new_df2=new_df1.swaplevel(axis=1)
    #add a level to new_cm
    d={}
    d[1.5]=new_cm
    e=pd.concat(d,axis=1)

    #concatenate new_cm (after adding the level of idexing)
    new_df3=pd.concat([new_df2,e],axis=1).sort_index(axis=1)
    return new_df3









def continuous_maturity_one_month(monthly_wide_records : pd.DataFrame)->pd.DataFrame:   
    """
    produces a weighted mean of the two nearest monthly futures resulting in an average of maturity one month.
    parameters:
    -----------
    monthly_records:
        A DataFrame in a wide format of Monthly records, as returned by  pivot_futures_on_monthly_tenor     
    """
    
    df=vix_futures_trade_dates_and_expiry_dates()

  

    futures_history=pd.DataFrame(monthly_wide_records)

 
       

 
    weights_all=vix_constant_maturity_weights(df)

    #we only need the weigths we for dates we have trades
    weights=weights_all[weights_all.index.isin(futures_history.index)]
   
    #select the front two months and the columns that have trade values
    with pd.option_context('display.max_columns',None): 
        logging.debug(f"\n{'*'*50}\nColumns to weight:\n{futures_history}")
    front_two_months=futures_history[[1,2]]
    futures_history_trade_value_columns=front_two_months.swaplevel(axis=1)[_weighted_column_names].swaplevel(axis=1)
 
    weighted_values=do_weighting_front_two_months(futures_history_trade_value_columns,weights)

    #should have the same number of rows
    assert weighted_values.shape[0]==futures_history_trade_value_columns.shape[0]

    
    df_file=front_two_months.swaplevel(axis=1)["File"]
    weighted_values["File"]=df_file[1]+"+"+df_file[2] 
    
    weighted_values['Expiry']=weights['Expiry']


    weighted_values["Tenor_Days"]=(weighted_values['Expiry']-weighted_values.index).dt.days


    return weighted_values