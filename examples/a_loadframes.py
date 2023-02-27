import pandas as pd
import vix_utils.download_vix_futures as v
import vix_utils
import asyncio as aio
import vix_utils.vix_cash_term_structure as cash
vix_futures_history=v.load_vix_term_structure()

#load it again using asyncio. required if calling from a coroutine. 
#get the cash term structure and futures history  

async def do_load():
    async with aio.TaskGroup() as tg:
        t1=tg.create_task(v.async_load_vix_term_structure())
        t2=tg.create_task(cash.get_vix_index_histories())

    return (t1.result(),t2.result())

vix_futures_history,vix_cash_history=aio.run(do_load())

print(f"Vix cash history \n{vix_cash_history}")
#this function same as wide_cash.all_vix_cash.pivot(index='Trade Date', columns="Symbol")

wide_cash=cash.pivot_on_trade_date(vix_cash_history)
print(f"Wide Vix cash history\n{wide_cash}")

print(f"The entire VIX Futures History:\n{vix_futures_history}")



#just the monthly
monthly=vix_futures_history[vix_futures_history['Weekly'] == False]

print(f"Just the monthly futures:\n{monthly}")

pivoted= monthly.set_index(["Trade Date","MonthTenor"]).unstack()
pivoted.columns.reorder_levels(order=[1,0])
pivoted=pivoted[["Close","File"]]
print(f"The monthlys, with a tenor column index, just a few columns:\n{pivoted}")
print(f"The column index for the stacked frame above:\n{pivoted.columns}")

