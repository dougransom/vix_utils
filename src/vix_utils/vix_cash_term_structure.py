import pandas as pd
import vix_utils.futures_utils as u
import logging as logging
from urllib.parse import urlparse
import aiofiles
import aiohttp
import asyncio
import io
from itertools import chain



# def _symbol_to_url(sym: str) -> str:
#     """

#     :param sym:  A symbol of a CBOE published index
#     :return: the URL to download the index historical data from.

#     Works for some of the CBOE indexes.
#     You can find a variety of indexes using the CBOE global index search.
#     https://ww2.cboe.com/index  and even more useful all on one page https://www.cboe.com/us/indices/indicesproducts/.
#     """
#     return f"https://ww2.cboe.com//publish/scheduledtask/mktdata/datahouse/{sym}_History.csv"


# _stu = _symbol_to_url           # save some typing

# use this URL to browse and find the index data
_cboe_indexes = "https://www.cboe.com/index/indexes"
_vix1y_dashboard = "https://www.cboe.com/index/dashboard/vix1y"


async def get_vix_index_histories(data_directory):

    # the variious fix_..columns rename columns of to be consistent with "Close" and "Trade Date"
    # as the various data files from CBOE aren't consistent
    def fix_vvix_columns(df):
        df = df.rename(columns={"VVIX": "Close", "Date": "Trade Date"})
        return df

    def fix_vix9d_columns(df):
        df = df.rename(columns={"Date": "Trade Date"})
        # Known bad date string in the data for april 20, 2011, looks like "*4/20/2011",
        # at least as of november 11, 2020
        td = df["Trade Date"]
        m = td == "*4/20/2011"
        # creates a pandas warning warning.  so there is likely a better way
        td.loc[m] = "4/20/2011"
        return df

    def fix_vix3m_columns(df):
        df.columns = ["Trade Date", "Open", "High", "Low", "Close"]
        return df

    fix_vix_columns = fix_vix3m_columns  # these are the same
    fix_vix_6m_columns = fix_vix9d_columns  # these are the same

    # fix_vix1y_columns not currently used because we  haven't developed a programmatic way to retrieve VIX1Y from CBOE
    def fix_vix1y_columns(df):
        df = df.iloc[:, 0:6]  # get rid of the cruft bogus columns
        # the columns in the file have whitespace.
        df.columns = ["time", "vol", "open", "high", "low", "close"]
        df = df[['time', 'open', 'high', 'low', 'close']]

        df.columns = ["Trade Date", "Open", "High", "Low", "Close"]
        return df

    def fix_one_value_column_result(df):
        df.columns = ['Trade Date', 'Close']
        return df

    # these symbols have  a trade date, close, and predictable URL
#    simple_data_symbols = ["RVOL", "RVOL3M", "RVOL6M", "RVOL12M"]

#    simple_data_urls = [_stu(sym) for sym in simple_data_symbols]
#    simple_data_lines_to_discard = [1]*len(simple_data_urls)
 #   simple_data_fixups = [fix_one_value_column_result]*len(simple_data_urls)
    symbols_with_value_only=['VVIX','GVZ']

    symbols_with_high_low_close=['VIX', 'VIX9D', "VIX3M", "VIX6M" ]
    index_history_symbols = symbols_with_value_only + symbols_with_high_low_close  
    index_history_urls = [f"https://cdn.cboe.com/api/global/us_indices/daily_prices/{symbol}_History.csv" for symbol in index_history_symbols]

    index_history_files= [data_directory/f"{symbol}_History.csv" for symbol in index_history_symbols]
    value_only_count=len(symbols_with_value_only)

    index_history_files_with_value_only=index_history_files[:value_only_count]
    index_history_files_with_high_low_close=index_history_files[value_only_count:]

    def add_symbol_and_set_index(frame, symbol):
        """

        :param frame: a data frame with at least a trade date column
        :param symbol: the symbol for the data in this table
        :return: the data frame, modified to have an index of Trade Date and a column "Symbol" with the symbol.
        """
        frame["Symbol"] = symbol
        frame["Trade Date"] = pd.to_datetime(frame["Trade Date"])
        frame.set_index("Trade Date")
        return frame

    async with aiohttp.ClientSession() as session:

        async def download_csv_from_web(url):
            """

 
            :return: returns nothin when the data have been downloaded into local storage.
   
            """
            logging.debug(f"\nReading URL {url}")
            # save the csv files for inspection.
            cache_file_name = urlparse(url).path.split('/')[-1]

            async with session.get(url) as resp:
                text = await resp.text()
            cache_file_path=data_directory/cache_file_name
            logging.debug(f"\nWriting file   {cache_file_path} ")   
            
            async with aiofiles.open(cache_file_path, mode='w', newline='') as f:
                await f.write(text)
            logging.debug(f"Wrote {cache_file_path}")

            return
        
                
        # download all of them
        logging.debug(f"Skipping read from web")
#        download_coro = (download_csv_from_web(url) for url in index_history_urls)
#        l = await asyncio.gather(*download_coro)


    def read_index_csv(fname,col_names,symbol):
        return pd.read_csv(fname,header=0,names=col_names).assign(Symbol=symbol)

    frames1=(read_index_csv(fname,["Trade Date","Close"],sym)  for 
        fname,sym in zip(index_history_files_with_value_only,symbols_with_value_only))
         
    frames2=(read_index_csv(fname,['Trade Date','Open','High','Low','Close'],sym) for  
       fname,sym in zip(index_history_files_with_high_low_close,symbols_with_high_low_close))
    

    frames=chain(frames1,frames2)
 

    all_vix_cash = pd.concat(frames)
    all_vix_cash['Trade Date'] = pd.to_datetime(all_vix_cash['Trade Date'])

    logging.debug(f"\nAll Vix cash \n{all_vix_cash}")
    all_cash_frame = all_vix_cash.pivot(index='Trade Date', columns="Symbol")

    logging.debug(f"stacked \n{all_cash_frame['Close']}")

    return all_cash_frame
