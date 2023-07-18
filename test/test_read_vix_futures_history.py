import pytest
from vix_utils.download_vix_futures import read_csv_future_file,read_csv_future_files
from vix_utils import load_vix_term_structure,get_vix_index_histories
import pandas as pd
from pathlib import Path
from shutil import copy,copytree
 
thisDir = Path(__file__).parent
futures_sample_input_dir=thisDir/"data_samples"
import inspect

#note the fname portion of dest_folder in the code below is being truncated.
#no need to fix it.

def function_name():
    """Return the name of the caller"""
    fname=inspect.getouterframes(inspect.currentframe())[1].function
    print(f"Function name{fname}")
    return fname

def test_read_future_file_may_2008_june1_2006(tmp_path):
    foldername="200705_200805_futures"
    fname=function_name()
    dest_folder=tmp_path/fname/foldername
    print(f"dest folder:{dest_folder}\nfunction name:{fname}")
    copytree(futures_sample_input_dir/foldername,dest_folder)

    futures_file=dest_folder/"2008-05-21.m_5.CFE_VX_K2008.csv"
    df=read_csv_future_file(futures_file, {})


def test_read_future_file_may_2008(tmp_path):
    foldername="200705_200805_futures"
    dest_folder=tmp_path/function_name()/foldername
    copytree(futures_sample_input_dir/foldername,dest_folder)

    futures_file=dest_folder/"2008-05-21.m_5.CFE_VX_K2008.csv"
    df=read_csv_future_file(futures_file, {})
    df2=df.set_index(['Trade Date'])
    df3=df2.loc['2006-06-01']
 
    Tenor_Monthly=df3["Tenor_Monthly"]
    assert Tenor_Monthly==24
    #the monthly  reads don't remove some of the garbage
    #just check a  trade not in the file doesn't have data

    #check we have removed garbage
    for date_str in ['2006-04-22']:
        with pytest.raises(KeyError):
            bogus_data=df2.loc[date_str]
            print(f"\n{date_str}: Bogus data:\n{bogus_data}")

def test_read_future_file_may_2007(tmp_path):
    foldername="200705_200805_futures"
    dest_folder=tmp_path/function_name()/foldername
    copytree(futures_sample_input_dir/foldername,dest_folder)

    futures_file=dest_folder/"2007-05-16.m_5.CFE_VX_K2007.csv"
    df=read_csv_future_file(futures_file, {})
    df2=df.set_index(['Trade Date'])

    df3=df2.loc['2006-06-01']

    Tenor_Monthly=df3["Tenor_Monthly"]
    assert Tenor_Monthly==12
    #the monthly  reads don't remove some of the garbage
    #just check a  trade not in the file doesn't have data

    #check we have removed garbage
    for date_str in ['2006-04-22']:
        with pytest.raises(KeyError):
            bogus_data=df2.loc[date_str]
            print(f"\n{date_str}: Bogus data:\n{bogus_data}")
   
def test_all_dates():
    pass
    spot=get_vix_index_histories().set_index("Trade Date")
    sv=spot[spot.Symbol=='SHORTVOL'][["Close"]]
    print(f"sv\n{sv}")
    ts=load_vix_term_structure().set_index("Trade Date")
    print(f"ts\n{ts}")
    diff1=sv.index.difference(ts.index).to_list()
    diff2=ts.index.difference(sv.index).to_list()
    diff3=ts.index.symmetric_difference(sv.index).to_list()
    if(len(diff3) >0 ):
        print(f"Missing from spot\n{diff1}\nMissing from Futures\n{diff2}\nMissing from either\n{diff3}")
        pass  #make it easy to put a breakpoint
    assert len(diff3) == 0


test_all_dates()