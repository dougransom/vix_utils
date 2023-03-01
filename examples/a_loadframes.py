import pandas as pd
import vix_utils.download_vix_futures as v
import vix_utils
import asyncio as aio
import vix_utils.vix_cash_term_structure as cash
import logging
import sys

#set up logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
stars='*'*80
def pstars(): print(stars)



async def do_load():
    async with aio.TaskGroup() as tg:
        t1=tg.create_task(v.async_load_vix_term_structure())
        t2=tg.create_task(cash.get_vix_index_histories())

    return (t1.result(),t2.result())

#create an event loop just for the do_load example

vix_futures_history,vix_cash_history=aio.run(do_load())

pstars()
print(f"Vix Cash History \n{vix_cash_history}\nVix Cash History index\n{vix_cash_history.columns}")
#this function same as wide_cash.all_vix_cash.pivot(index='Trade Date', columns="Symbol")

#you can do it this way if you don't have a running event loop (ie. you aren't using asyncio)
vix_futures_history=v.load_vix_term_structure()

pstars()
wide_cash=cash.pivot_on_trade_date(vix_cash_history)
print(f"Wide Vix Cash history\n{wide_cash}\nWide Cash History Index\n{wide_cash.columns}")

pstars()
print(f"\nThe entire VIX Futures History:\n{vix_futures_history}")



#just the monthly
monthly=v.select_monthly(vix_futures_history)

vix_futures_history[vix_futures_history['Weekly'] == False]

pstars()
print(f"Just the monthly futures:\n{monthly}")

pstars()
pivoted= v.pivot_on_monthly_tenor(monthly)
pivoted=pivoted[['Close','File']]
print(f"The monthlys, with a tenor column index, just a few columns:\n{pivoted}\ncolumn_index{pivoted.columns}")

pstars()
