import vix_futures_term_struture as v
import vix_cash_term_structure as cash
import pandas as pd

quandl_api_key="5cDGQqduzQgmM_2zfkd1"

if __name__ == "__main__":
    #not normal to import here
    print("""Warning:  matplotlib.pyplot and scipy.stats are required to run this script.  They are not prerequisites of"""
    """ this package, so you may have to install them into your environment""")
    
    import matplotlib.pyplot as plt
    import scipy.stats as bc

    cash.get_vix_index_histories("file:VIX1Y_Data.csv")
    futures_term_structure = v.vix_futures_term_structure(quandl_api_key,3)
    plt.plot(futures_term_structure['Close'])
    plt.show()






