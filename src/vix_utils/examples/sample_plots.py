"""
Example use of vixutil to plot the term structure.
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

    skipPlot=False       #set to false when you want to see plots
                        #allows you to skip to a specific plot when toying with the script
    def plotDF(df):
        if skipPlot:
            print(f"\n{stars}Warning, skipPlot for:\n{df}\n{stars}\n")
            return
        df.plot()
        plt.show()
        
    logging.getLogger().debug("setting pandas options")
    with pd.option_context(*pd_options):
    
        logging.getLogger().debug("set pandas options")


        vix_futures_skinny = vix_utils.load_vix_term_structure()
        vix_futures_monthly_skinny=vix_utils.select_monthly_futures(vix_futures_skinny)
        vix_futures_wide=vix_utils.pivot_futures_on_monthly_tenor(vix_futures_monthly_skinny)
        vix_spot=vix_utils.get_vix_index_histories()
        vix_spot_wide=vix_utils.pivot_spot_term_structure_on_symbol(vix_spot)
        
        sep_lines = "_"*25+"\n"





        logging.getLogger().debug(f"Futures wide columns:\n{vix_futures_wide.columns}")

        #we just want 9 tenors for now
        selected_tenors=list(range(1,10))
        original_vix_futures_wide=pd.DataFrame(vix_futures_wide)
        vix_futures_wide=vix_futures_wide[selected_tenors]

        close=vix_futures_wide.swaplevel(0,1,axis=1)[["Close"]]

        logging.getLogger().debug(f"\nclose columns: {close.columns}")

        plotDF(close)

        logging.getLogger().debug(f"{stars}\noriginal Vix futures wide columns:\n{original_vix_futures_wide.columns}")
        m1m2_weighted=vix_utils.continuous_maturity_one_month(original_vix_futures_wide)
        logging.getLogger().debug(f"{stars}\nm1m2 weighted:\n{m1m2_weighted}\ncolumns:\n{m1m2_weighted.columns}")

        wide_with_continuous_futures=vix_utils.append_continuous_maturity_one_month(original_vix_futures_wide)
        #front two months
        wide_with_continuous_futures_f2m=\
            wide_with_continuous_futures[[1,2,1.5]].swaplevel(axis=1)[["Close"]].swaplevel(axis=1) 

        plotDF(wide_with_continuous_futures_f2m)        


        logging.getLogger().debug(f"{stars}\nSpot vix\n{vix_spot_wide}")
        b=vix_spot_wide['Close'][['VIX3M','VIX','VIX9D','VIX1D']]
        plotDF(b)

        c=vix_spot_wide['Close'][['VXTLT','GVZ','VVIX','OVX']]
        plotDF(c)

        d=vix_spot_wide['Close'][['SHORTVOL']]
        plotDF(d)

        e=vix_spot_wide['Close'][['LONGVOL']]
        plotDF(e)


        #plot the term structure for Feb 16, 2021
        day_of_interest = '2023-07-05'
        df_day_of_interest =wide_with_continuous_futures.loc[[day_of_interest]]
        cols_to_plot=[1,1.5] +  list(range(2,10))
        df_day_of_interest_to_plot=df_day_of_interest.swaplevel(axis=1)[['Close',"Tenor_Days"]].swaplevel(axis=1)[cols_to_plot].swaplevel(axis=1)
        df_debug=wide_with_continuous_futures[[1,1.5,2]].swaplevel(axis=1)[['Close','Tenor_Days','Expiry']]

        with pd.option_context("display.max_columns",None,"display.max_rows",None):
            print(f"df_day_of_interest{stars}\ndf_day_of_interest")
            print(f"{stars}\ndf_day_of_interest_to_plot:\n{df_day_of_interest_to_plot}")
            print(f"Columns\n{df_day_of_interest.columns}")
            print(f"Index:\n{df_day_of_interest.index}")
            print(f"df_debug\n{df_debug}")
        #we really need the days until expiry as well, as the x-axis.
        skipPlot=False
        if not skipPlot:
            df_day_of_interest_to_plot.plot(x="Tenor_Days", y="Close", kind = 'scatter', use_index=True)
            plt.show()
            df_s=df_day_of_interest_to_plot.stack(1)
            df_s.plot.line(x="Tenor_Days", y="Close")
            plt.show()
            spot_di=vix_spot_wide.loc[day_of_interest]["Close"][["VIX9D","VIX","VIX3M","VIX6M"]]

            print(f"\nspot\n{spot_di}")
            spot_df_di=pd.DataFrame(index=[9,30,60,180],data=spot_di.values,columns=['Close'])
            print(f"\nspot df di\n{spot_df_di}")
            spot_df_di.plot.line(y='Close')
            plt.show()

import logging
if __name__=="__main__":
    logging.getLogger("vix_utils")
    main()
