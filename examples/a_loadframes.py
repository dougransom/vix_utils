import pandas as pd
import vix_utils.download_vix_futures as v
import vix_utils
import asyncio as aio

vix_futures_history=v.load_vix_term_structure()

#load it again using asyncio. required if calling from a coroutine. 

async def do_load():
    return await v.async_load_vix_term_structure()

vix_futures_history=aio.run(do_load())

print(f"The entire VIX Futures History:\n{vix_futures_history}")



#just the monthly
monthly=vix_futures_history[vix_futures_history['Weekly'] == False]

print(f"Just the monthly futures:\n{monthly}")

pivoted= monthly.set_index(["Trade Date","MonthTenor"]).unstack()
pivoted.columns.reorder_levels(order=[1,0])
pivoted=pivoted[["Close","File"]]
print(f"The monthlys, with a tenor column index, just a few columns:\n{pivoted}")
print(f"The column index for the stacked frame above:\n{pivoted.columns}")

