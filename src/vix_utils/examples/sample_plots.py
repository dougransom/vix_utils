"""
Example use of vixutil to plot the term structure.
Be sure to run vixutil -r first to download the data.
"""
import vix_utils 
import pandas as pd
import logging  
import asyncio
import sys
import matplotlib.pyplot as plt
import scipy.stats as bc
stars='*'*80
def pstars():
    """Print a line of '*' """ 
    print(stars)
def main():    
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    print(f"\n{stars}Running python file:\n{__file__}\n")
    pd_options=['display.max_rows', 10, 
    'display.min_rows', 10,
    'display.max_columns', None,
    'display.width', None,
    'display.max_colwidth', None]

    logging.getLogger().debug("setting pandas options")
    with pd.option_context(*pd_options):
    
        logging.getLogger().debug("set pandas options")


        vix_futures_skinny = vix_utils.load_vix_term_structure()
        vix_futures_monthly_skinny=vix_utils.select_monthly_futures(vix_futures_skinny)
        vix_futures_wide=vix_utils.pivot_futures_on_monthly_tenor(vix_futures_monthly_skinny)
        vix_cash=vix_utils.get_vix_index_histories()
        vix_cash_wide=vix_utils.pivot_cash_term_structure_on_symbol(vix_cash)
        
        sep_lines = "_"*25+"\n"





        logging.getLogger().debug(f"Futures wide columns:\n{vix_futures_wide.columns}")

        #we just want 9 tenors for now
        selected_tenors=list(range(1,10))
        original_vix_futures_wide=pd.DataFrame(vix_futures_wide)
        vix_futures_wide=vix_futures_wide[selected_tenors]

        close=vix_futures_wide.swaplevel(0,1,axis=1)[["Close"]]

        logging.getLogger().debug(f"\nclose columns: {close.columns}")

        close.plot()
        plt.show()

        logging.getLogger().debug(f"{stars}\noriginal Vix futures wide columns:\n{original_vix_futures_wide.columns}")
        m1m2_weighted=vix_utils.continuous_maturity_30day(original_vix_futures_wide)
        logging.getLogger().debug(f"{stars}\nm1m2 weighted:\n{m1m2_weighted}\ncolumns:\n{m1m2_weighted.columns}")

        wide_with_continuous_futures=vix_utils.append_continuous_maturity_30day(original_vix_futures_wide)
        wide_with_continuous_futures_f2m=\
            wide_with_continuous_futures[[1,2,"30 Day Continuous"]].swaplevel(axis=1)[["Close"]].swaplevel(axis=1) 
        wide_with_continuous_futures_f2m.plot()
        plt.show()


        logging.getLogger().debug(f"{stars}\nCash vix\n{vix_cash_wide}")
        b=vix_cash_wide['Close'][['VIX3M','VIX','VIX9D']]
        b.plot()
        plt.show()

        #plot the term structure for Feb 16, 2021
        day_of_interest = '2021-02-16'
        df_day_of_interest =wide_with_continuous_futures.loc[[day_of_interest]]
        cols_to_plot=[1] + ["30 Day Continuous"] + list(range(2,10))
        df_day_of_interest_to_plot=df_day_of_interest.swaplevel(axis=1)[['Close',"Tenor_Days"]].swaplevel(axis=1)[cols_to_plot].swaplevel(axis=1)
        print(f"{stars}\ndf_day_of_interest_to_plot:\n{df_day_of_interest_to_plot}")
        #we really need the days until expiry as well, as the x-axis.
        df_day_of_interest_to_plot.plot(x="Tenor_Days", y="Close", kind = 'scatter', use_index=True)
        plt.show()
if __name__=="__main__":
    main()
