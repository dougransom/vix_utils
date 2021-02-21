"""
Example use of vixutil to plot the term structure.
Be sure to run vixutil -r first to download the data.
"""
import vixutil as vutil

import pandas as pd
import logging as logging
import asyncio
import sys



pd.set_option('display.max_rows', 10)
#need over two months
pd.set_option('display.min_rows', 10)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


logger=logging.getLogger()
logger.setLevel(logging.INFO)




vutils=vutil.VixUtilsApi()
weights=vutils.get_vix_futures_constant_maturity_weights()
constant_maturity_term_structure = vutils.get_vix_futures_constant_maturity_term_structure()
cash_vix = vutils.get_cash_vix_term_structure()
futures_term_structure = vutils.get_vix_futures_term_structure()
wide_vix_calendar=vutils.get_vix_futures_constant_maturity_weights()

sep_lines = "_"*25+"\n"



constant_maturity_weights=vutils.get_vix_futures_constant_maturity_weights()
try:
    import matplotlib.pyplot as plt
    import scipy.stats as bc
except Exception as e:
    logging.warning(f"""Exception {e} while trying to plot.  matplotlip and scipy.stats 
                    are required to run the plots in this example. Install them into your environment if you want to
                    see the graphs.""")

    sys.exit(-3)
# the nine month has some bad data in it
#futures_term_structure = futures_term_structure.swaplevel(0,1,axis=1).drop(columns=[9]).swaplevel(0, 1, axis=1)
#futures_term_structure.drop(level=1,columns=[9,8],inplace=True)
futures_term_structure[['Close']].plot()

#            futures_term_structure[['VIX1M_SPVIXSTR','Close']].plot()
plt.show()

constant_maturity_term_structure[['Close']].plot()
print(f"Constant maturity term structure {constant_maturity_term_structure}")
plt.show()

print(f"Cash vix {cash_vix}")
b=cash_vix['Close'][['VIX3M','VIX','VIX9D']]
b.plot()
plt.show()

#plot the term structure for Feb 16, 2021
day_of_interest = '2021-02-16'
s1 = futures_term_structure.loc[day_of_interest][["Close", "Settlement Date"]]
s2 = constant_maturity_term_structure.loc[day_of_interest][["Close", "Settlement Date"]]

s1.index = pd.Index([ (a,f"{b}") for a,b in s1.index])
s3=pd.concat([s1,s2])
one_day_ts = pd.DataFrame(s3).unstack(0)
iii=one_day_ts.columns.droplevel(0)
one_day_ts.columns=iii
one_day_ts.sort_values("Settlement Date",inplace=True)
print(f"{one_day_ts}")
one_day_ts.plot(x="Settlement Date", y="Close", kind = 'scatter', use_index=True)
plt.show()
