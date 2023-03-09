import pandas as pd

import vix_utils as v
import asyncio as aio

import logging
import sys
stars='*'*80
def pstars():
    """Print a line of '*' """ 
    print(stars)

def main():
    logging.basicConfig(stream=sys.stdout, level=logging.WARN)

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
    wide_cash=v.pivot_cash_term_structure_on_symbol(vix_cash_history)
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
    pivoted=pivoted.swaplevel(0,1,axis=1)

    pivoted=pivoted[['Close','File']]
    print(f"The monthlys, with a tenor column index, levels swapped, just a few columns:\n{pivoted}\ncolumn_index{pivoted.columns}")

    pstars()
    m1m2_weighted=v.continuous_maturity_30day(monthly)
    print(f"\nm1m2 weighted:\n{m1m2_weighted}")

if __name__=="__main__":
    main()