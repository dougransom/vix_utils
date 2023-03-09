"""
Example use of vixutil to plot the term structure.
Be sure to run vixutil -r first to download the data.
"""
import vix_utils 
import pandas as pd
import logging as logging
import asyncio
import sys

stars='*'*80
def pstars():
    """Print a line of '*' """ 
    print(stars)
def main():    
    print(f"\n{stars}Running python file:\n{__file__}\n")
    pd.set_option('display.max_rows', 10)
    #need over two months
    pd.set_option('display.min_rows', 10)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)


    logger=logging.getLogger()
    logger.setLevel(logging.INFO)



    vix_futures_skinny = vix_utils.load_vix_term_structure()
    vix_futures_monthly_skinny=vix_utils.select_monthly_futures(vix_futures_skinny)
    vix_futures_wide=vix_utils.pivot_futures_on_monthly_tenor(vix_futures_monthly_skinny)
    vix_cash=vix_utils.get_vix_index_histories()
    vix_cash_wide=vix_utils.pivot_cash_term_structure_on_symbol(vix_cash)
    
    sep_lines = "_"*25+"\n"


    import matplotlib.pyplot as plt
    import scipy.stats as bc


    logging.debug(f"{vix_futures_wide.columns}")

    #we just want 9 tenors for now
    selected_tenors=list(range(1,10))
    vix_futures_wide=vix_futures_wide[selected_tenors]

    close=vix_futures_wide.swaplevel(0,1,axis=1)[["Close"]]

    logging.debug(f"\nclose columns: {close.columns}")

    close.plot()
    plt.show()


    logging.debug(f"Cash vix {vix_cash_wide}")
    b=vix_cash_wide['Close'][['VIX3M','VIX','VIX9D']]
    b.plot()
    plt.show()

    #plot the term structure for Feb 16, 2021
    day_of_interest = '2021-02-16'
    s1 = futures_term_structure.loc[day_of_interest][["Close", "Expiry"]]
    s2 = constant_maturity_term_structure.loc[day_of_interest][["Close", "Expiry"]]

    s1.index = pd.Index([ (a,f"{b}") for a,b in s1.index])
    s3=pd.concat([s1,s2])
    one_day_ts = pd.DataFrame(s3).unstack(0)
    iii=one_day_ts.columns.droplevel(0)
    one_day_ts.columns=iii
    one_day_ts.sort_values("Expiry",inplace=True)
    print(f"{one_day_ts}")
    one_day_ts.plot(x="Expiry", y="Close", kind = 'scatter', use_index=True)
    plt.show()

if __name__=="__main__":
    main()
