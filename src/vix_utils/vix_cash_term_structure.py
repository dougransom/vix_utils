import pandas as pd
import vix_utils.futures_utils as u
import logging as logging
from urllib.parse import urlparse
import aiofiles
import aiohttp
import asyncio
import io

# https://ww2.cboe.com/publish/scheduledtask/mktdata/datahouse/vixcurrent.csv
_vix_index_history = "https://ww2.cboe.com/publish/scheduledtask/mktdata/datahouse/vixcurrent.csv"
_vvx_history = "https://ww2.cboe.com/publish/scheduledtask/mktdata/datahouse/vvixtimeseries.csv"
_vix9d_history = "https://ww2.cboe.com/publish/scheduledtask/mktdata/datahouse/vix9ddailyprices.csv"
_vix3m_history = "https://ww2.cboe.com/publish/scheduledtask/mktdata/datahouse/vix3mdailyprices.csv"
_vix6m_history = "https://ww2.cboe.com/publish/scheduledtask/mktdata/datahouse/vix6mdailyprices.csv"
# this is kind of a fragile way to get it, but no data published as above

_gvz_history = "https://ww2.cboe.com/publish/scheduledtask/mktdata/datahouse/gvzhistory.csv"

# todo add symbols OVX,VSLV,VXGDX,VXXLE,VXN

# and VOLI/VOLQ


# in theory but the web server won't provide the data to the python code.  More sophisticated
# scraping required.  so not used.
# _vix1y_history = "https://www.cboe.com/chart/GetDownloadData/?RequestSymbol=VIX1Y"


def _symbol_to_url(sym: str) -> str:
    """

    :param sym:  A symbol of a CBOE published index
    :return: the URL to download the index historical data from.

    Works for some of the CBOE indexes.
    You can find a variety of indexes using the CBOE global index search.
    https://ww2.cboe.com/index  and even more useful all on one page https://www.cboe.com/us/indices/indicesproducts/.
    """
    return f"https://ww2.cboe.com//publish/scheduledtask/mktdata/datahouse/{sym}_History.csv"


_stu = _symbol_to_url           # save some typing

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
    simple_data_symbols = ["RVOL", "RVOL3M", "RVOL6M", "RVOL12M"]

    simple_data_urls = [_stu(sym) for sym in simple_data_symbols]
    simple_data_lines_to_discard = [1]*len(simple_data_urls)
    simple_data_fixups = [fix_one_value_column_result]*len(simple_data_urls)

    index_history_urls = [_vix_index_history, _vvx_history, _vix9d_history, _vix3m_history, _vix6m_history,
                          _gvz_history] + simple_data_urls
    index_history_symbols = ['VIX', 'VVIX', 'VIX9D', "VIX3M", "VIX6M", "GVZ"] + simple_data_symbols

    # various files from CBO have lines above the CSV data that need to be tossed.
    num_lines_to_discard = [1, 1, 3, 2, 2, 1] + simple_data_lines_to_discard
    # the function to fixup the columns is passed in to the function that builds the data frame
    fixups = [fix_vix_columns, fix_vvix_columns, fix_vix9d_columns, fix_vix3m_columns,
              fix_vix_6m_columns, fix_one_value_column_result] + simple_data_fixups

    z = list(zip(index_history_urls, index_history_symbols, num_lines_to_discard, fixups))

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

        async def read_csv_from_web(url, lines_to_discard):
            """

            :param url:
            :param lines_to_discard:
            :return: a data from from reading the data at url, discarding lines_to_discard lines
            before parsing the CSV into a DataFrame.  The CSV file is also saved.
            """
            logging.debug(f"\nReading URL {url} lines_to_discard {lines_to_discard}")
            # save the csv files for inspection.
            cache_file_name = urlparse(url).path.split('/')[-1]

            async with session.get(url) as resp:
                text = await resp.text()
            logging.debug(f"\nWriting file   {cache_file_name} ")   
            
            async with aiofiles.open(data_directory/cache_file_name, mode='w', newline='') as f:
                await f.write(text)

            input_stream = io.StringIO(text)
            print(f"{cache_file_name}:  {text[0:500]}")
            frame = pd.read_csv(input_stream, header=lines_to_discard)
            print(f"{cache_file_name} frame: \n{frame}")
            return frame
        # frames with the columns fixed
        frames_coro = (read_csv_from_web(url, n) for (url, sym, n, f) in z)
        frames_unfixed = await asyncio.gather(*frames_coro)
        # fix up the columns (renaming them to be consistent, add the symbols as a column)
        frames_z = list(zip(frames_unfixed,  index_history_symbols, fixups))
        frames = list(add_symbol_and_set_index(f(t_frame), sym) for (t_frame, sym, f) in frames_z)

    all_vix_cash = pd.concat(frames)
    all_vix_cash['Trade Date'] = pd.to_datetime(all_vix_cash['Trade Date'])

    logging.debug(f"\nAll Vix cash \n{all_vix_cash}")
    all_cash_frame = all_vix_cash.pivot(index='Trade Date', columns="Symbol")

    logging.debug(f"stacked \n{all_cash_frame['Close']}")

    return all_cash_frame
