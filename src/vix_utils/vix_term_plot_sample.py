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

futures_term_structure[['Close']].plot()

#            futures_term_structure[['VIX1M_SPVIXSTR','Close']].plot()
plt.show()
constant_maturity_term_structure[['Close']].plot()
plt.show()

print(f"Cash vix {cash_vix}")
b=cash_vix['Close'][['VIX3M','VIX','VIX9D']]
b.plot()
plt.show()

print(f"{b}")


