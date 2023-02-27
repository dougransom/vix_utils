import pandas as pd
import vix_utils.download_vix_futures as v
import vix_utils

vix_futures_history=v.load_vix_term_structure()

print(f"The entire VIX Futures History:\n{vix_futures_history}")



#just the monthly
monthly=vix_futures_history[vix_futures_history['Weekly'] == False]

print(f"Just the monthly futures:\n{monthly}")

pivoted= monthly.set_index(["Trade Date","MonthTenor"]).unstack()
pivoted.columns.reorder_levels(order=[1,0])
pivoted=pivoted[["Close","File"]]
print(f"The monthlys, with a tenor column index, just a few columns:\n{pivoted}")
print(f"The columns for the stacked frame above:  {pivoted.columns}")

#monthly.set_index(["Trade Date","MonthTenor"]).unstack()
                  
#from the old way
#@u.timeit()
#def pivot_on_contract_maturity(df):
#    return df.reset_index().pivot(columns="Contract Month", index="Trade Date")
