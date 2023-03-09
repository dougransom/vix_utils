"""
This module provides both the command line program and a Python interface to provide
the VIX futures term structure, the VIX continuous maturity
term structure, and the VIX cash term structure.

"""
import argparse
from appdirs import user_data_dir,user_log_dir
 
import pandas as pd
import logging as logging
import asyncio
import io
import aiofiles
import os.path as ospath
import pathlib
import configparser
from pathlib import Path
import vix_utils


extensions = [".csv", ".pkl",  ".xlsx", ".html"]  # supported output file types

parser = argparse.ArgumentParser()
output_format_help = f"""The file extension determines the file type. Valid extensions are: {extensions}.
\nPython programmers may prefer to use the API """

 

parser.add_argument("-f", dest="futures_records",
                    help=f"""output the history of vix futures to a file in record format. Includes weekly and monthly expiries.
                    {output_format_help}""")

parser.add_argument("-g", dest="futures_wide", help=f"""output the history of vix monthly expiry futures in wide format, with a column for each tenor.  
                    {output_format_help}""")

parser.add_argument("-w", dest="continuous_weights", help=f"""output the weights of the various vix futures front two months 
    to make a 30 day average tenor.   
    Note the weights are as of the beginning of the trading day.  {output_format_help}""")

parser.add_argument("-c", dest="cash_records", help=f"""output the vix cash term structure a file in record format. 
        {output_format_help}.  Some other indexes from CBOE
        will also be included.  {output_format_help} """)
parser.add_argument("-d", dest="cash_wide", help=f"""output the vix cash term structure a file in wide format,with a column for each index. 
        {output_format_help}.  Some other indexes from CBOE
        will also be included.  {output_format_help} """)


parser.add_argument("--calendar", dest="calendar", help="Expirys for vix futures for a given trade date")

parser.add_argument("--loglevel",dest="loglevel",choices=["DEBUG","INFO","WARNING","ERROR", "CRITICAL"], help=
                    f"Level for logging module to display, default is ERROR",
                    default="ERROR")
                    

async def write_frame_ex(frame, ofile, functions):


    extension_to_function_map = dict(zip(extensions, functions))
    suffix = pathlib.Path(ofile).suffix
    if suffix in extension_to_function_map:
        fn = extension_to_function_map[suffix]
        outstream=io.StringIO()
        fn(outstream)
        with aiofiles.open(ofile,mode='b') as f:
            await f.write(outstream.buffer)
    else:
        print(f"Unsupported extension, only {extensions} are supported")


async def write_frame(frame,ofile):
    functions = [frame.to_csv, frame.to_pickle,  frame.to_excel, frame.to_html]
    return await write_frame_ex(frame,ofile,functions)



 
def main():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    args = parser.parse_args()
    logger.setLevel(args.loglevel)

    vix_futures=vix_utils.get_vix_index_histories()
    vix_cash=vix_utils.get_vix_index_histories()
    vix_monthly_futures_wide=vix_utils.pivot_cash_term_structure_on_symbol(vix_futures)
    vix_cash_wide=vix_utils.pivot_cash_term_structure_on_symbol(vix_cash)

    if ofile := args.futures_records:
        write_frame(vix_futures, ofile)

    if ofile := args.futures_wide:
        write_frame(vix_monthly_futures_wide, ofile)

    if ofile := args.cash_records:
        write_frame(vix_cash,ofile)

    if ofile := args.cash_wide:
        write_frame(vix_cash_wide,ofile)


 #TODO   if ofile := args.continuous:
 #         write_frame(cmt, ofile)



#    if ofile := args.calendar:
#        calendar = vutils.get_vix_trade_and_future_settlements()[selection]
#        write_frame(calendar, ofile)
#todo

# todo   
# if ofile := args.continuous_weights:
#        weights = vutils.get_vix_futures_constant_maturity_weights()[selection]
#        write_frame(weights, ofile)

    return 0

 
if __name__ == "__main__":
    main()
