import argparse
import vix_futures_term_struture as v
import vix_cash_term_structure as cash
import pandas as pd
import logging as logging
import asyncio
import os.path as ospath

quandl_api_key="5cDGQqduzQgmM_2zfkd1"
from pathlib import Path

def _vix_util_data_Path():
    """
    :return: the path where VIX term structure and calendar data are stored.
    """
    user_path=Path.home()
    print(user_path)
    vixutil_path = user_path / ".vixutil"
    vixutil_path.mkdir(exist_ok=True)
    return vixutil_path

_vix_futures_constant_maturity_term_structure_file="vix_futures_constant_maturity_term_struture.pkl"
_vix_futures_term_structure_file="vix_futures_term_struture.pkl"
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

        download_quandl_coro=asyncio.to_thread(v.download_quandle_data,quandl_api_key,self.data_path)
        wide_vix_calendar_coro = asyncio.to_thread(v.vix_futures_trade_dates_and_settlement_dates())
        ch = asyncio.create_task(cash.get_vix_index_histories())
        cash_vix = await ch
        wide_vix_calendar = await wide_vix_calendar_coro
        await download_quandl_coro
        wide_vix_calendar.to_pickle(self.data_path/"wide_vix_calendar.pkl")
        cash_vix.to_pickle(self.data_path / "vix_cash_term_structure.pkl")
        #v.download_quandle_data(quandl_api_key,p)

#        vt=v.vix_futures_term_structure(p,wide_vix_calendar)
#       vt.to_pickle(self.data_path /"vix_futures_term_structure.pkl")


        def get_cash_vix_term_structure(self):
            """Return the cash vix term structure.  """
            return pd.from_pickle(self.data_path/"vix_cash_term_struture.pkl")

        def get_vix_trade_and_future_settlements(self):
            return pd.from_pickle(self.data_path/"wide_vix_calendar.pkl")

        def _make_vix_futures_term_structure(self):
            wide_vix_calendar = self.get_vix_trade_and_future_settlements()
            vt = v.vix_futures_term_structure(self.data_path,wide_vix_calendar)
            vt.to_pickle(self.data_path/_vix_futures_term_structure_file)
            return vt

        def _needs_update(self,output_file):
            #use the cached version if the code hasn't changed.  This makes development much
            #easier.
            return not ospath.exists(output_file) or ospath.mtime(output_file) < ospath.getmtime(v.__file__)

        def get_or_make_helper(self,filename,make_method):
            if _needs_update(filename):
                df=make_method()
                df.to_pickle(filename)
            else:
                df=pd.read_pickle(filename)
            return df

        def get_vix_futures_term_structure(self):
            f=self.data_path/_vix_futures_term_structure_file
            return self.get_or_make_helper(f,self._make_vix_futures_term_structure)


        def get_vix_futures_constant_maturity_term_structure(self):
            f=self.data_path/_vix_futures_constant_maturity_term_structure_file

            def _make_vix_futures_constant_maturity_term_structure(self):
                return v.vix_continuous_maturity_term_structure(self.get_vix_trade_and_future_settlements(),
                                                                get_vix_futures_term_structure())

            return self.get_or_make_helper(f,_make_vix_futures_constant_maturity_term_structure)

        def get_vix_futures_constant_maturity_weights(self):
            f=self.data_path/"vix_futures_constant_maturity_weights.pkl"
            def make_weights(): return v.vix_constant_maturity_weights(self.get_vix_trade_and_future_settlements())
            return self.get_or_make_helper(f,make_weights)



parser=argparse.ArgumentParser()
output_format_help="""The file extension determines the file type. Valid extensions are:
      * xslx  for excel.
      * pkl  for pickle format.  Python programmers are better off to use the API.
      * csv for csf format"""
parser.add_argument("-i",help = "information about where the data is stored")
parser.add_argument("-r", help = "rebuild the vix futures term structure and vix cash term stucture",action="store_true")
parser.add_argument("-t",  help =
    f"""output the vix futures term structure to a file. {output_format_help}""")
parser.add_argument("-m",  help =
    f"""output the vix continuous maturity (i.e. interpolated) futures term structure to a file. {output_format_help}""")

parser.add_argument("-c",  help=
f"""output the vix cash maturity (i.e. interpolated) futures term structure to a file. {output_format_help}.  Some other indexes will from CBOE
be included.  {output_format_help} """)



def main():
    args=parser.parse_args()
    if args.r:
        print("Rebuild")
        vutils=VixUtilsApi()
        asyncio.run(vutils.rebuild())
        print("Rebuilt")

main()