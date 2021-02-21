"""
This module provides both the command line program and a Python interface to provide
the VIX futures term structure, the VIX continuous maturity
term structure, and the VIX cash term structure.

"""
import argparse
import vix_utils.vix_futures_term_struture as v
import vix_utils.vix_cash_term_structure as cash
import pandas as pd
import logging as logging
import asyncio
import os.path as ospath
import pathlib
import configparser
from pathlib import Path
from vix_utils.futures_utils import timeit

quandl_api_key = f"This is not a valid quandle key {__file__}"


_override_data_path = False


def _set_config_path(str_path):
    global _override_data_path
    _override_data_path = str_path


def _vix_util_data_path():
    """
    :return: the path where VIX term structure and calendar data are stored.
    """
    if _override_data_path:
        return Path(_override_data_path)
    user_path = Path.home()
    vixutil_path = user_path / ".vixutil"
    vixutil_path.mkdir(exist_ok=True)
    return vixutil_path


def _needs_update(output_file):
    # use the cached version if the code hasn't changed.  This makes development much
    # easier.
    return not ospath.exists(output_file) or ospath.getmtime(output_file) < ospath.getmtime(v.__file__)


_vix_futures_constant_maturity_term_structure_file = "vix_futures_constant_maturity_term_structure.pkl"
_vix_futures_term_structure_file = "vix_futures_term_struture.pkl"
_vix_cash_file = "vix_cash_term_structure.pkl"


class VixUtilsApi:
    """
    Attributes:  data_path.  The path folder of the downloaded and pre-prepared
    data files.
    """

    def __init__(self, *data_dir):
        """
        :param data_dir:  optional override of the default data dir where
        files are stored by the library.  The default with will be in the the .vixutils subdirectory of the users home
        directory.
        """

        self.data_path = _vix_util_data_path() if len(data_dir) == 0 else data_dir[0]

    async def rebuild(self):
        """
        Downloads the files for vix term structures, and generates a map from trade days to each future settlment date.
        Most users will prefer to to use the command line program to perform this.
        :return:  nothing
        """

        download_quandl_coro = asyncio.to_thread(v.download_quandle_data, quandl_api_key, self.data_path)
        ch = asyncio.create_task(cash.get_vix_index_histories(self.data_path))
        wide_vix_calendar_coro = asyncio.to_thread(v.vix_futures_trade_dates_and_settlement_dates)
        (cash_vix, _, wide_vix_calendar) = await asyncio.gather(ch, download_quandl_coro, wide_vix_calendar_coro)

        wide_vix_calendar.to_pickle(self.data_path / "wide_vix_calendar.pkl")
        cash_vix.to_pickle(self.data_path / _vix_cash_file)

    def get_cash_vix_term_structure(self):
        """Return the cash vix term structure.  """
        return pd.read_pickle(self.data_path / _vix_cash_file)

    def get_vix_continuous_future_weights(self):
        return v.vix_constant_maturity_weights(self.get_vix_trade_and_future_settlements())

    def get_vix_trade_and_future_settlements(self):
        return pd.read_pickle(self.data_path / "wide_vix_calendar.pkl")

    def _make_vix_futures_term_structure(self):
        wide_vix_calendar = self.get_vix_trade_and_future_settlements()
        vt = v.vix_futures_term_structure(self.data_path, wide_vix_calendar)
        vt.to_pickle(self.data_path / _vix_futures_term_structure_file)
        return vt

    @staticmethod
    def get_or_make_helper(filename, make_callable):
        if _needs_update(filename):
            df = make_callable()
            df.to_pickle(filename)
        else:
            df = pd.read_pickle(filename)
        return df

    def get_vix_futures_term_structure(self):
        f = self.data_path / _vix_futures_term_structure_file
        return self.get_or_make_helper(f, self._make_vix_futures_term_structure)

    @timeit()
    def get_vix_futures_constant_maturity_weights(self):
        f = self.data_path / "vix_futures_constant_maturity_weights.pkl"

        def make_weights(): return v.vix_constant_maturity_weights(self.get_vix_trade_and_future_settlements())

        return self.get_or_make_helper(f, make_weights)

    @timeit()
    def get_vix_futures_constant_maturity_term_structure(self):
        f = self.data_path / _vix_futures_constant_maturity_term_structure_file

        def _make_vix_futures_constant_maturity_term_structure():
            return v.vix_continuous_maturity_term_structure(self.get_vix_trade_and_future_settlements(),
                                                            self.get_vix_futures_term_structure())

        return self.get_or_make_helper(f, _make_vix_futures_constant_maturity_term_structure)


extensions = [".csv", ".pkl",  ".xlsx", ".html"]  # supported output file types

parser = argparse.ArgumentParser()
output_format_help = f"""The file extension determines the file type. Valid extensions are: {extensions}.
\n  Python programmers may prefer to use the API """

parser.add_argument("--config_dir", dest="config_dir",
                    help="store the config file and other files in this folder instead of the default.")
parser.add_argument("-i", help="information about where the data is stored", dest='info', action='store_true')
parser.add_argument("-s", help='Store  Quandl API Key supplied with -q in config file ', dest="store_quandle_api_key",
                    action='store_true')
parser.add_argument("-q", help='Quandl API Key', dest='quandl_api_key')

parser.add_argument("-r",
                    help="""download the data from Quandl and CBOE and rebuild the vix futures term 
                    structure and vix cash term structure""",
                    action="store_true", dest='rebuild')

parser.add_argument("-t", dest="term_structure",
                    help=f"""output the vix futures term structure to a file. {output_format_help}""")

parser.add_argument("-m", dest="continuous", help=f"""output the vix continuous maturity (i.e. interpolated) 
                    futures term structure to a file.
                    {output_format_help}""")

parser.add_argument("-w", dest="continuous_weights", help=f"""output the weights of the various vix futures tenors 
    required to interpolate vix continuous maturity futures.   
    Note the weights are as of the beginning of the trading day.  {output_format_help}""")

parser.add_argument("-c", dest="cash", help=f"""output the vix cash term structure a file. 
        {output_format_help}.  Some other indexes from CBOE
        will also be included.  {output_format_help} """)

parser.add_argument("--calendar", dest="calendar", help="settlement dates for vix futures for a given trade date")
parser.add_argument("--start_date", dest="start_date", help="iso format date YYYY-MM-DD, exclude any dates prior")
parser.add_argument("--end_date", dest="end_date", help="iso format date, YYYY-MM-DD exclude any dates after")


def read_config_file():
    config_file_path = _vix_util_data_path() / 'vixutil.config'
    cp = configparser.ConfigParser()
    cp.read(config_file_path)
    global quandl_api_key
    if "QUANDLE" in cp:
        quandl_api_key = cp['QUANDLE']['QUANDLE_API_KEY']


def write_config_file():
    global quandl_api_key
    config_file_path = _vix_util_data_path() / 'vixutil.config'
    cp = configparser.ConfigParser()
    cp['QUANDLE'] = {'QUANDLE_API_KEY': quandl_api_key}
    with open(config_file_path, 'w') as configfile:
        cp.write(configfile)


def write_frame_ex(frame, ofile, functions):


    extension_to_function_map = dict(zip(extensions, functions))
    suffix = pathlib.Path(ofile).suffix
    if suffix in extension_to_function_map:
        fn = extension_to_function_map[suffix]
        fn(ofile)
    else:
        print(f"Unsupported extension, only {extensions} are supported")


def write_frame(frame,ofile):
    functions = [frame.to_csv, frame.to_pickle,  frame.to_excel, frame.to_html]
    return write_frame_ex(frame,ofile,functions)





def main():
    global quandl_api_key
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    args = parser.parse_args()

    start_date = pd.to_datetime(start_date_str) if (start_date_str := args.start_date) else None
    end_date = pd.to_datetime(end_date_str) if (end_date_str := args.end_date) else None

    selection = slice(start_date, end_date)   # start_date and end_date are not required to be initialized

    # this must happen before reading the configuration file.
    if args.config_dir:
        o_data_path = args.config_dir
        _set_config_path(o_data_path)

    read_config_file()

    if args.info:
        print(f"Data and config file are stored in {_vix_util_data_path()}")

    if args.quandl_api_key:
        quandl_api_key = args.quandl_api_key

    if args.store_quandle_api_key:
        write_config_file()

    vutils = VixUtilsApi()
    if args.rebuild:
        print("Rebuilding data files from Quandl and CBOE")
        timeit(logging.INFO)(asyncio.run)(vutils.rebuild())
        print("Rebuilt Files")

    if ofile := args.term_structure:
        fts = vutils.get_vix_futures_term_structure()[selection]
        write_frame(fts, ofile)

    if ofile := args.continuous:
        cmt = vutils.get_vix_futures_constant_maturity_term_structure()[selection]
        write_frame(cmt, ofile)

    if ofile := args.cash:
        cash_term_structure = vutils.get_cash_vix_term_structure()[selection]
        write_frame(cash_term_structure, ofile)

    if ofile := args.calendar:
        calendar = vutils.get_vix_trade_and_future_settlements()[selection]
        write_frame(calendar, ofile)

    if ofile := args.continuous_weights:
        weights = vutils.get_vix_futures_constant_maturity_weights()[selection]
        write_frame(weights, ofile)

    return 0


main()
