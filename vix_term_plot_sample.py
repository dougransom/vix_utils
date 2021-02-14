import vix_futures_term_struture as v
import vix_cash_term_structure as cash
import pandas as pd
import logging as logging
import asyncio


quandl_api_key="5cDGQqduzQgmM_2zfkd1"

load_wide_vix_calendar_from_cache=True
load_from_cache=False

#pd.set_option('display.max_rows', )
#need over two months
pd.set_option('display.min_rows', 100)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


#if __name__ == "__main__":

if True:

    loop = asyncio.get_event_loop()

    # define a Handler which writes INFO messages or higher to the sys.stderr
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
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



    logging.info(f"Wide Vix Calendar")

    if not load_wide_vix_calendar_from_cache:
        wide_vix_calendar=v.vix_futures_trade_dates_and_settlement_dates()
        wide_vix_calendar.to_pickle("wide_vix_calendar.pkl")
    else:
        wide_vix_calendar = pd.read_pickle("wide_vix_calendar.pkl")

    if not load_from_cache:
        futures_term_structure = v.vix_futures_term_structure(quandl_api_key,wide_vix_calendar,9)
        cash_vix = loop.run_until_complete(cash.get_vix_index_histories())
        futures_term_structure.to_pickle("futures_term_structure.pkl")
        cash_vix.to_pickle("cash_term_structure.pkl")
        logging.info("Saved futures term structure to cache")
    else:
        futures_term_structure = pd.read_pickle("futures_term_structure.pkl")
        cash_vix = pd.read_pickle("cash_term_structure.pkl")
        logging.info("Loaded term structure from cache")


#    logging.info(f"\nVix Futures Term Structure {futures_term_structure}")

#    ft2=v.vix_constant_maturity(futures_term_structure)

    logging.info(f"Vix Futures Weights for continuous maturities")
    ft3=v.vix_constant_maturity_weights(wide_vix_calendar)

    ft4=v.vix_continuous_maturity_term_structure(wide_vix_calendar,futures_term_structure)
    print(f"ft4\n{ft4}")

    if True:
        try:
            import matplotlib.pyplot as plt
            import scipy.stats as bc


            futures_term_structure[['Close']].plot()

#            futures_term_structure[['VIX1M_SPVIXSTR','Close']].plot()
            plt.show()
            ft4[['Close']].plot()
            plt.show()
#            print(f"cash vix\n{cash_vix}")
            a=ft2[['VIX1M_SPVIXSTR']]
            b=cash_vix['Close'][['RVOL', 'VIX']]
            b['VIX1M_SPVIXSTR']=a['VIX1M_SPVIXSTR']

            b.sort_index()
            c=b['RVOL']-b['VIX1M_SPVIXSTR']
#            plt.show()
            print(f"A\n{a} \nB\n{b}___\n")

            c.plot()
            plt.show()

        except Exception as e:
             logger.warn(f"""Exception {e} while trying to plot.  matplotlip and scipy.stats 
                        are required to run the plots in this example. Install them into your environment if you want to
                        see the graphs.""")
        loop.close()





