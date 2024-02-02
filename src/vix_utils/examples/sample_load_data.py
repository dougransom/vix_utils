import pandas as pd

import vix_utils as v
import asyncio as aio

import logging
import sys
from itertools import chain
stars='*'*80
def pstars(toprint=""):
    """Print a line of '*' """ 
    print(f"\n{stars}\n{toprint}")

def main():
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    print(f"\n{stars}Running python file:\n{__file__}\n")
    async def do_load():
        async with aio.TaskGroup() as tg:
            t1=tg.create_task(v.async_load_vix_term_structure())
            t2=tg.create_task(v.async_get_vix_index_histories())

        return (t1.result(),t2.result())

    #create an event loop just for the do_load example

    vix_futures_history,vix_cash_history=aio.run(do_load())

    pstars()
    print(f"Vix Cash History \n{vix_cash_history}\nVix Cash History index\n{vix_cash_history.columns}")
    #this function same as wide_cash.all_vix_cash.pivot(index='Trade Date', columns="Symbol")

    #you can do it this way if you don't have a running event loop (ie. you aren't using asyncio)
    vix_futures_history=v.load_vix_term_structure()

    pstars()
    wide_cash=v.pivot_spot_term_structure_on_symbol(vix_cash_history)
    print(f"Wide Vix Cash history\n{wide_cash}\nWide Cash History Index\n{wide_cash.columns}")

    pstars()
    print(f"\nThe entire VIX Futures History:\n{vix_futures_history}")



    #just the monthly
    monthly=v.select_monthly_futures(vix_futures_history)
    

    pstars()
    print(f"Just the monthly futures:\n{monthly}")

    pstars()
    pivoted= v.pivot_futures_on_monthly_tenor(monthly)
    print(f"\npivoted {pivoted}")

    pstars() 
    indexed_by_tenor=vix_futures_history.set_index(["Trade Date","Tenor_Monthly"])
    print(f"indexed by tenor:\n{indexed_by_tenor}")


    pivoted_swapped=pivoted.swaplevel(0,1,axis=1)

    pivoted_two_cols=pivoted_swapped[['Close','File']]
    olhc=["Open","High","Low","Close"]
    pivoted_ohlc=pivoted_swapped[olhc]
    vix_ohlc=wide_cash.swaplevel(0,1,axis=1)[["VIX"]].swaplevel(0,1,axis=1)
    #get the columns correspondenting to futures tenors

    #replicate in the spot prices
    
 

   
    m1m2_weighted=v.continuous_maturity_one_month(pivoted)
    pstars(f"\nm1m2 weighted:\n{m1m2_weighted}\ncolumns:\n{m1m2_weighted.columns}")

    appended_m1m2=v.append_continuous_maturity_one_month(pivoted)
    appended_m1m2_close=appended_m1m2[[1,1.5,2]].swaplevel(axis=1)[['Close','Tenor_Days','Expiry']]
    pstars(f"\nappended_m1m2:\n{appended_m1m2}")

    closes=appended_m1m2.swaplevel(axis=1)["Close"]

    pstars(f"\ncloses\n{closes}")
    print(f"\nappended m1m2 to wide (close):\n{appended_m1m2_close}")


    vix_cash_history_closes=wide_cash["Close"]
    spot_symbols=["VIX9D","VIX","VIX3M","GVZ"]  #some symbols to compare with the VIX futures for a basis.
                                                #the basis that makes sense of course is on the VIX, since the vix futures 
                                                #are for the VIX spot settlement.
                                                #there reasonably should be a relationship with the various
                                                #vix spot indexes and probably a weaker one with GVZ.



    def add_column_level(df:pd.DataFrame,var_name):
        df2=pd.DataFrame(df)
        idx=df2.columns.to_frame()
        idx.insert(0,"Variable",var_name)
        df2.columns=pd.MultiIndex.from_frame(idx)
        return df2 

    vix_basis_by_index=[add_column_level(closes.sub(vix_cash_history_closes[spot_symbol],axis=0),spot_symbol+"_Basis") for spot_symbol in ["VIX9D","VIX","VIX3M","GVZ"]]

    closes=add_column_level(closes,"Futures")

    vix_basis=pd.concat(chain([closes],vix_basis_by_index),axis=1,join="inner")
    
    pstars(f"vix_basis{vix_basis}")
   
 
 
    with pd.option_context("display.max_rows",None,"display.max_columns",None):   
        df2021_02=appended_m1m2.loc['2021-02'][[1,1.5,2]].swaplevel(axis=1)[['Close','Tenor_Days','Expiry']]
        print(f"\nappended (2021-02)\n{df2021_02}")

 
if __name__=="__main__":
    main()