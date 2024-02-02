import pandas as pd

import logging as logging
from urllib.parse import urlparse
import aiofiles
import aiohttp
import asyncio
import io
from itertools import chain
from appdirs import user_log_dir
from .location import data_dir,make_dir
from pathlib import Path


# use this URL to browse and find the index data
_cboe_indexes = "https://www.cboe.com/index/indexes"

def get_vix_index_histories():
    """
    Return the history of some volatility indexes.
    """
 
#    with asyncio.Runner() as runner:
#        return runner.run(async_get_vix_index_histories())
    return asyncio.run(async_get_vix_index_histories())

async def async_get_vix_index_histories():
    """
    Return the history of some volatility indexes.
    """

    data_directory= Path(data_dir())

    cash_data_directory=data_directory/"cash"
    download_data_directory=cash_data_directory/"download"
    del data_directory        
    make_dir(download_data_directory)
    symbols_with_value_only=['VVIX','GVZ','OVX','SHORTVOL','LONGVOL','VXTLT']

    symbols_with_high_low_close=['VIX', 'VIX9D', "VIX3M", "VIX6M","VIX1D" ]
    index_history_symbols = symbols_with_value_only + symbols_with_high_low_close  
    index_history_urls = [f"https://cdn.cboe.com/api/global/us_indices/daily_prices/{symbol}_History.csv" for symbol in index_history_symbols]

    index_history_files= [download_data_directory/f"{symbol}_History.csv" for symbol in index_history_symbols]
    value_only_count=len(symbols_with_value_only)

    index_history_files_with_value_only=index_history_files[:value_only_count]
    index_history_files_with_high_low_close=index_history_files[value_only_count:]


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
            cache_file_path=download_data_directory/cache_file_name

            logging.debug(f"\nWriting file   {cache_file_path} ")   
            
            async with aiofiles.open(cache_file_path, mode='w', newline='') as f:
                await f.write(text)
            logging.debug(f"Wrote {cache_file_path}")

            return
        
                
        # download all of them

        download_coro = (download_csv_from_web(url) for url in index_history_urls)
        l = await asyncio.gather(*download_coro)


    def read_index_csv(fname,col_names,symbol):
        return pd.read_csv(fname,header=0,names=col_names).assign(Symbol=symbol)

    frames1=(read_index_csv(fname,["Trade Date","Close"],sym)  for 
        fname,sym in zip(index_history_files_with_value_only,symbols_with_value_only))
         
    frames2=(read_index_csv(fname,['Trade Date','Open','High','Low','Close'],sym) for  
       fname,sym in zip(index_history_files_with_high_low_close,symbols_with_high_low_close))
    

    frames=chain(frames1,frames2)
 

    all_vix_spot = pd.concat(frames)
    all_vix_spot['Trade Date'] = pd.to_datetime(all_vix_spot['Trade Date'])
    all_vix_spot.set_index('Trade Date')
    logging.debug(f"\nAll Vix spot \n{all_vix_spot}")
 
    return all_vix_spot 

def pivot_spot_term_structure_on_symbol(all_vix_cash):
    try:           
        m1=f"all_vix_cash columns index:\n{all_vix_cash.columns}"
        all_spot_frame = all_vix_cash.set_index(["Trade Date","Symbol"]).unstack()
    except Exception as e:
        logging.error("{e} in pivot_spot_term_structure_on_trade_date\n{m1}\n{m1}")
        raise e

    return all_spot_frame