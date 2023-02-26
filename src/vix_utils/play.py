import pandas as pd

df=pd.read_pickle("C:/Users/doug/AppData/Local/.vixutil/skinny.pkl")
print(f"df {df}")
weekly=df[df['Weekly']] 
monthly=df[df['Weekly'] == False]

from . import __version__
print(f"Version {version}")
#monthly.set_index(["Trade Date","MonthTenor"]).unstack()
                  
#from the old way
#@u.timeit()
#def pivot_on_contract_maturity(df):
#    return df.reset_index().pivot(columns="Contract Month", index="Trade Date")
