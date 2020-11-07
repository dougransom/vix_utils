import vix_futures_term_struture as v
import vix_cash_term_structure as cash
import pandas as pd
import logging as logging

quandl_api_key="5cDGQqduzQgmM_2zfkd1"
load_from_cache=True


#pd.set_option('display.max_rows', )
#need over two months
pd.set_option('display.min_rows', 100)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


#if __name__ == "__main__":

if True:


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




    if not load_from_cache:
        futures_term_structure = v.vix_futures_term_structure(quandl_api_key,3)
        cash_vix = cash.get_vix_index_histories("file:VIX1Y_Data.csv")
        futures_term_structure.to_pickle("futures_term_structure.pkl")
        cash_vix.to_pickle("cash_term_structure.pkl")
        logging.info("Saved futures term structure to cache")
    else:
        futures_term_structure = pd.read_pickle("futures_term_structure.pkl")
        cash_vix = pd.read_pickle("cash_term_structure.pkl")
        logging.info("Loaded term structure from cache")

    logging.info(f"\nVix Futures Term Structure {futures_term_structure}")
    v.vix_1m_term_structure(futures_term_structure)
    if False:
        try:
            import matplotlib.pyplot as plt
            import scipy.stats as bc


            plt.plot(futures_term_structure['Close'])
            plt.plot(cash_vix['Close'])
            plt.show()
        except Exception as e:
             logger.warn(f"""Exception {e} while trying to plot.  matplotlip and scipy.stats 
                        are required to run the plots in this example. Install them into your environment if you want to
                        see the graphs.""")





