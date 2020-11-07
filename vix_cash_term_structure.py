import  pandas as pd
import utils.futures_utils as u
import logging as logging

_vix_index_history = "http://www.cboe.com/publish/scheduledtask/mktdata/datahouse/vixcurrent.csv"
_vvx_history ="http://www.cboe.com/publish/scheduledtask/mktdata/datahouse/vvixtimeseries.csv"
_vix9d_history= "http://www.cboe.com/publish/scheduledtask/mktdata/datahouse/vix9ddailyprices.csv"
_vix3m_history = "http://www.cboe.com/publish/scheduledtask/mktdata/datahouse/vix3mdailyprices.csv"
_vix6m_history = "http://www.cboe.com/publish/scheduledtask/mktdata/datahouse/vix6mdailyprices.csv"
#this is kind of a fragile way to get it, but no data published as above

_gvz_history = "http://www.cboe.com/publish/scheduledtask/mktdata/datahouse/gvzhistory.csv"

#todo add symbols OVX,VSLV,VXGDX,VXXLE,VXN

#and VOLI/VOLQ


#in theory but the web server won't provide the data to the python code.  More sophisticated
#scraping required.  so not used.
_vix1y_history = "https://www.cboe.com/chart/GetDownloadData/?RequestSymbol=VIX1Y"


def symbol_to_url(sym):
    ''' Works for some of the CBOE indexes'''
    return f"http://www.cboe.com//publish/scheduledtask/mktdata/datahouse/{sym}_History.csv"

stu=symbol_to_url           #safe some typing

# use this URL to browse and find the index data
cboe_indexes= "https://www.cboe.com/index/indexes"
vix1y_dashboard="https://www.cboe.com/index/dashboard/vix1y"


def get_vix_index_histories(vix1y_url):

    def fix_vvix_columns(df):
        df = df.rename(columns={"VVIX": "Close", "Date": "Trade Date"})
        return df

    def fix_vix9d_columns(df):
        df = df.rename(columns={"Date": "Trade Date"})
        #Known bad date string in the data for april 20, 2011, looks like "*4/20/2011", at least as of november 11, 2020
        td=df["Trade Date"]
        m= td == "*4/20/2011"
        #creates a pandas warning warning.  so there is likely a better way
        td.loc[m]="4/20/2011"
        return df

    def fix_vix3m_columns(df):
        df.columns = ["Trade Date", "Open", "High", "Low", "Close"]
        return df

    fix_vix_columns = fix_vix3m_columns  # these are the same
    fix_vix_6m_columns = fix_vix9d_columns  # these are the same

    def fix_vix1y_columns(df):
        df = df.iloc[:, 0:6]  # get rid of the cruft bogus columns
        # the columns in the file have whitespace.
        df.columns = ["time", "vol", "open", "high", "low", "close"]
        df = df[['time', 'open', 'high', 'low', 'close']]

        df.columns = ["Trade Date", "Open", "High", "Low", "Close"]
        return df

    def fix_one_value_column_result(df):
        df.columns=['Trade Date','Close']
        return df

    #ones that just have  a trade date, close, and predicatable URL
    simple_data_symbols=["RVOL","RVOL3M","RVOL6M","RVOL12M","SMILE"]

    simple_data_urls=[stu(sym) for sym in simple_data_symbols]
    simple_data_lines_to_discard=[1]*len(simple_data_urls)
    simple_data_fixups = [fix_one_value_column_result]*len(simple_data_urls)

    index_history_urls = [_vix_index_history, _vvx_history, _vix9d_history, _vix3m_history, _vix6m_history,vix1y_url,_gvz_history]+\
    simple_data_urls
    index_history_symbols = ['VIX', 'VVIX', 'VIX9D', "VIX3M", "VIX6M","VIX1Y","GVZ"]+simple_data_symbols

    num_lines_to_discard = [1, 1, 3, 2, 2,2,1]+simple_data_lines_to_discard


    # the function to fixup the columns is passed in to the function that builds the data frame
    fixups = [fix_vix_columns, fix_vvix_columns, fix_vix9d_columns, fix_vix3m_columns,
              fix_vix_6m_columns,fix_vix1y_columns,fix_one_value_column_result]+simple_data_fixups

    z = list(zip(index_history_urls, index_history_symbols, num_lines_to_discard, fixups))

    def add_symbol_and_set_index(frame, symbol):
        frame["Symbol"] = symbol
        frame["Trade Date"]=pd.to_datetime(frame["Trade Date"])
        frame.set_index("Trade Date")
        return frame
    @u.timeit()
    def read_csv_from_web(url,lines_to_discard):
        logging.debug(f"\nReading URL {url} lines_to_discard {lines_to_discard}")
        frame= u.timeit()(pd.read_csv)(url,header=lines_to_discard)
        return frame

    #quandl limits concurrency, no chance to optimize by reading all the data concurrently.
    frames =  (add_symbol_and_set_index(f(read_csv_from_web(url,n)),sym) for (url,sym,n,f) in z)

    frames=list(frames)
    all_vix_cash = pd.concat(frames)
    all_vix_cash['Trade Date']=pd.to_datetime(all_vix_cash['Trade Date'])

    logging.debug(f"\nAll Vix cash \n{all_vix_cash}")
    df = all_vix_cash.pivot(index='Trade Date',columns="Symbol")

    logging.debug(f"stacked \n{df['Close']}")
    logging.warn(f"Warning, vix1y being read from {vix1y_dashboard}, you have to manually update it ")



    return df
