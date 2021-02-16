import vix_futures_term_struture as v
import vix_cash_term_structure as cash
import vixutil as vutil

import pandas as pd
import logging as logging
import asyncio


quandl_api_key="5cDGQqduzQgmM_2zfkd1"

load_wide_vix_calendar_from_cache=False
load_vix_futures_from_cache=False
load_vix_cash_from_cache=False

#pd.set_option('display.max_rows', )
#need over two months
pd.set_option('display.min_rows', 10)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

#if __name__ == "__main__":

if True:

 #   loop = asyncio.get_event_loop()

    # define a Handler which writes INFO messages or higher to the sys.stderr
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logging.log(logging.DEBUG,"Debug message")
    logging.log(logging.INFO, "Info message")
    logging.info("Info Message 2")
    logging.warning("warning message")
    logging.error("error message")
    logging.critical("Critical Message")

    # not normal to import here
    logging.warn(
        """Warning:  matplotlib.pyplot and scipy.stats are required to run this script.  They are not prerequisites of"""
        """ this package, so you may have to install them into your environment""")

    vutils=vutil.VixUtilsApi()
    weights=vutils.get_vix_futures_constant_maturity_weights()
    constant_maturity_term_structure = vutils.get_vix_futures_constant_maturity_term_structure()
    cash_vix = vutils.get_cash_vix_term_structure()
    futures_term_structure = vutils.get_vix_futures_term_structure()
    wide_vix_calendar=vutils.get_vix_futures_constant_maturity_weights()


    sep_lines = "_"*25+"\n"



    constant_maturity_weights=vutils.get_vix_futures_constant_maturity_weights

    if True:
        try:
            import matplotlib.pyplot as plt
            import scipy.stats as bc


            futures_term_structure[['Close']].plot()

#            futures_term_structure[['VIX1M_SPVIXSTR','Close']].plot()
            plt.show()
            constant_maturity_term_structure[['Close']].plot()
            plt.show()
#            print(f"cash vix\n{cash_vix}")
#            a=ft2[['VIX1M_SPVIXSTR']]
            b=cash_vix['Close'][['RVOL', 'VIX']]
            b['VIX1M_SPVIXSTR']=a['VIX1M_SPVIXSTR']

            b.sort_index()
            c=b['RVOL']-b['VIX1M_SPVIXSTR']
#            plt.show()
            print(f"A\n{a} \nB\n{b}___\n")

            c.plot()
            plt.show()

        except Exception as e:
             logger.warning(f"""Exception {e} while trying to plot.  matplotlip and scipy.stats 
                        are required to run the plots in this example. Install them into your environment if you want to
                        see the graphs.""")





