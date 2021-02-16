import argparse
import vix_utils.vix_futures_term_struture as v
import vix_utils.vix_cash_term_structure as cash
import pandas as pd
import logging as logging
import asyncio
import os.path as ospath
import pathlib
import configparser

from vix_utils.futures_utils import timeit


quandl_api_key=f"This is not a valid quandle key {__file__}"
from pathlib import Path

override_data_path=False



def _vix_util_data_Path():
    """
    :return: the path where VIX term structure and calendar data are stored.
    """
    if override_data_path:
        return Path(overide_data_path)
    user_path=Path.home()
    print(user_path)
    vixutil_path = user_path / ".vixutil"
    vixutil_path.mkdir(exist_ok=True)
    return vixutil_path

def _needs_update(output_file):
    #use the cached version if the code hasn't changed.  This makes development much
    #easier.
    return not ospath.exists(output_file) or ospath.getmtime(output_file) < ospath.getmtime(v.__file__)


_vix_futures_constant_maturity_term_structure_file="vix_futures_constant_maturity_term_structure.pkl"
_vix_futures_term_structure_file="vix_futures_term_struture.pkl"
_vix_cash_file="vix_cash_term_structure.pkl"
class VixUtilsApi:
    """
    Attributes:  data_path.  The path folder of the downloaded and pre-prepared
    data files.
    """
    def __init__(self,*data_dir):
        """
        :param data_dir:  optional override of the default data dir where
        files are stored by the library.  The default with will be in the the .vixutils subdirectory of the users home
        directory.
        """

        self.data_path=_vix_util_data_Path() if len(data_dir)== 0 else data_dir[0]


    async def rebuild(self):
        """
        Downloads the files for vix term structures, and generates a map from trade days to each future settlment date.
        Most users will prefer to to use the command line program to perform this.
        :return:  nothing
        """
        print("\nstarting")
        download_quandl_coro=asyncio.to_thread(v.download_quandle_data,quandl_api_key,self.data_path)

        wide_vix_calendar_coro = asyncio.to_thread(v.vix_futures_trade_dates_and_settlement_dates)
        ch = asyncio.create_task(cash.get_vix_index_histories(self.data_path))
        (cash_vix,_,wide_vix_calendar) = await asyncio.gather(ch,download_quandl_coro,wide_vix_calendar_coro)

        print("\finished waiting")


        wide_vix_calendar.to_pickle(self.data_path/"wide_vix_calendar.pkl")
        cash_vix.to_pickle(self.data_path / _vix_cash_file)


    def get_cash_vix_term_structure(self):
        """Return the cash vix term structure.  """
        return pd.read_pickle(self.data_path/_vix_cash_file)

    def get_vix_trade_and_future_settlements(self):
        return pd.read_pickle(self.data_path/"wide_vix_calendar.pkl")

    def _make_vix_futures_term_structure(self):
        wide_vix_calendar = self.get_vix_trade_and_future_settlements()
        vt = v.vix_futures_term_structure(self.data_path,wide_vix_calendar)
        vt.to_pickle(self.data_path/_vix_futures_term_structure_file)
        return vt


    def get_or_make_helper(self,filename,make_callable):
        if _needs_update(filename):
            df=make_callable()
            df.to_pickle(filename)
        else:
            df=pd.read_pickle(filename)
        return df

    def get_vix_futures_term_structure(self):
        f=self.data_path/_vix_futures_term_structure_file
        return self.get_or_make_helper(f,self._make_vix_futures_term_structure)

    @timeit()
    def get_vix_futures_constant_maturity_weights(self):
        f=self.data_path/"vix_futures_constant_maturity_weights.pkl"
        def make_weights(): return v.vix_constant_maturity_weights(self.get_vix_trade_and_future_settlements())
        return self.get_or_make_helper(f,make_weights)
    @timeit()
    def get_vix_futures_constant_maturity_term_structure(self):
        f=self.data_path/_vix_futures_constant_maturity_term_structure_file

        def _make_vix_futures_constant_maturity_term_structure():
            return v.vix_continuous_maturity_term_structure(self.get_vix_trade_and_future_settlements(),
                                                            self.get_vix_futures_term_structure())

        return self.get_or_make_helper(f,_make_vix_futures_constant_maturity_term_structure)





parser=argparse.ArgumentParser()
output_format_help="""The file extension determines the file type. Valid extensions are:
      * xslx  for excel.
      * pkl  for pickle format.  Python programmers are better off to use the API.
      * csv for csf format"""

parser.add_argument("-i",help = "information about where the data is stored",dest='info',action='store_true')
parser.add_argument("-s", help = 'Store  Quandle API Key supplied with -q in config file ',dest="store_quandle_api_key",action='store_true')
parser.add_argument("-q", help='Quandle API Key',dest='quandl_api_key')

parser.add_argument("-r", help = "rebuild the vix futures term structure and vix cash term stucture",action="store_true",dest='rebuild')
parser.add_argument("-t",  dest="term_structure",help =
    f"""output the vix futures term structure to a file. {output_format_help}""")
parser.add_argument("-m",  help =
    f"""output the vix continuous maturity (i.e. interpolated) futures term structure to a file. {output_format_help}""")

parser.add_argument("-c",  help=
f"""output the vix cash term structure a file. {output_format_help}.  Some other indexes will from CBOE
be included.  {output_format_help} """)


def read_config_file():
    config_file_path=_vix_util_data_Path()/'vixutil.config'
    cp=configparser.ConfigParser()
    cp.read(config_file_path)
    global quandl_api_key
    if "QUANDLE" in cp:
        quandl_api_key=cp['QUANDLE']['QUANDLE_API_KEY']

def write_config_file():
    global quandl_api_key
    config_file_path=_vix_util_data_Path()/'vixutil.config'
    cp=configparser.ConfigParser()
    cp['QUANDLE']={'QUANDLE_API_KEY': quandl_api_key}
    with open(config_file_path, 'w') as configfile:
        cp.write(configfile)


def main():
    global quandl_api_key
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    logging.log(logging.DEBUG,"Debug message")
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    read_config_file()
    args=parser.parse_args()

    if(args.info):
        print(f"Data and config file are stored in {_vix_util_data_Path()}")

    if(args.quandl_api_key):
        quandl_api_key=args.quandl_api_key()

    if args.store_quandle_api_key:
        write_config_file()



    vutils = VixUtilsApi()
    if args.rebuild:
        print("Rebuild")
        asyncio.run(vutils.rebuild())
        print("Rebuilt")
    vutils.get_vix_futures_constant_maturity_weights()
    cmt=vutils.get_vix_futures_constant_maturity_term_structure()
    cash=vutils.get_cash_vix_term_structure()
    fts=vutils.get_vix_futures_term_structure()

    extensions=[".csv",".pkl",".parquet",".hdf",".xlsx",".json",".html"]
    functions=[fts.to_csv,fts.to_pickle,fts.to_parquet,fts.to_hdf,fts.to_excel,fts.to_json,fts.to_html]
    extension_to_function_map=dict(zip(extensions,functions))

    if args.term_structure:
        ofile=args.term_structure
        print(f"Output file {ofile}")
        suffix = pathlib.Path(ofile).suffix
        if(suffix in extension_to_function_map):
            fn=extension_to_function_map[suffix]
            fn(ofile)
        else:
            print(f"Unsupported extension, only {extensions} are supported")


main()