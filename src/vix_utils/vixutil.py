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

 

parser.add_argument("-f", 
                    metavar="output_file",
                    dest="futures_records",
                    help=f"""output the history of vix futures to a file in record format. Includes weekly and monthly expiries.
                    {output_format_help}""")

parser.add_argument("-g",
                    metavar="output_file", 
                    dest="futures_wide", help=f"""output the history of vix monthly expiry futures in wide format, with a column for each tenor.  
                    {output_format_help}""")
parser.add_argument("-j", 
                    metavar="output_file",
                    dest="futures_m1m2", help=f"""weighted mean of the front two month vix futures for an average thirty day tenor .     {output_format_help}""")
parser.add_argument("-w", 
                    metavar="output_file",
                    dest="w_m1m2", help=f"""output the weights of the various vix futures front two months 
    to make a 30 day average tenor.   
    Note the weights are as of the beginning of the trading day.  {output_format_help}""")

parser.add_argument("-c",
                    metavar="output_file", 
                    dest="spot_records", help=f"""output the vix spot term structure a file in record format. 
        {output_format_help}.  Some other indexes from CBOE
        will also be included.  {output_format_help} """)
parser.add_argument("-d", 
                    metavar="output_file",
                    dest="spot_wide", help=f"""output the vix spot term structure a file in wide format,with a column for each index. 
        {output_format_help}.  Some other indexes from CBOE
        will also be included.  {output_format_help} """)


parser.add_argument("--calendar", metavar="output_file", dest="calendar", help="Expirys for vix futures for a given trade date")

parser.add_argument("--loglevel", metavar="output_file",dest="loglevel",choices=["DEBUG","INFO","WARNING","ERROR", "CRITICAL"], help=
                    f"Level for logging module to display, default is ERROR",
                    default="ERROR")
                    

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
    return  write_frame_ex(frame,ofile,functions)



 
def main():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    args = parser.parse_args()
    logger.setLevel(args.loglevel)

    vix_futures=vix_utils.load_vix_term_structure()
    vix_spot=vix_utils.get_vix_index_histories()
    vix_monthly_futures_wide=vix_utils.pivot_futures_on_monthly_tenor(vix_futures)
    vix_spot_wide=vix_utils.pivot_spot_term_structure_on_symbol(vix_spot)

    vix_m1m2_weights = vix_utils.vix_constant_maturity_weights(vix_utils.vix_futures_trade_dates_and_expiry_dates())
    futures_m1m2=vix_utils.continuous_maturity_one_month(vix_monthly_futures_wide)
    if ofile := args.futures_records:
        write_frame(vix_futures, ofile)

    if ofile := args.futures_wide:
        write_frame(vix_monthly_futures_wide, ofile)

    if ofile := args.spot_records:
        write_frame(vix_spot,ofile)

    if ofile := args.spot_wide:
        write_frame(vix_spot_wide,ofile)

    if ofile := args.w_m1m2:
        write_frame(vix_m1m2_weights,ofile)   
    
    if ofile := args.futures_m1m2:
        write_frame(futures_m1m2,ofile)   

 

    return 0

 
if __name__ == "__main__":
    main()
