import pytest
from vix_utils.download_vix_futures import read_csv_future_file,read_csv_future_files
import pandas as pd
from pathlib import Path

 
thisDir = Path(__file__).parent
futures_sample_dir=thisDir/"data_samples"/"200705_200805_futures"

def test_read_future_file_may_2008_june1_2006():
    futures_file=futures_sample_dir/"2008-05-21.m_5.CFE_VX_K2008.csv"
    df=read_csv_future_file(futures_file, {})
    print(f"\nrecord format\n{df}")


def test_read_future_file_may_2008():
    futures_file=futures_sample_dir/"2008-05-21.m_5.CFE_VX_K2008.csv"
    df=read_csv_future_file(futures_file, {})
    print(f"\nrecord format\n{df}")
    df2=df.set_index(['Trade Date'])
    print(f"\nResult:\n{df2}")
    df3=df2.loc['2006-06-01']
    print(f"\nResult:\n{df3}")

    monthTenor=df3["MonthTenor"]
    assert monthTenor==24
    #the monthly  reads don't remove some of the garbage
    #just check a  trade not in the file doesn't have data

    #check we have removed garbage
    for date_str in ['2006-04-22']:
        with pytest.raises(KeyError):
            bogus_data=df2.loc[date_str]
            print(f"\n{date_str}: Bogus data:\n{bogus_data}")

def test_read_future_file_may_2007():
    futures_file=futures_sample_dir/"2007-05-16.m_5.CFE_VX_K2007.csv"
    df=read_csv_future_file(futures_file, {})
    print(f"\nrecord format\n{df}")
    df2=df.set_index(['Trade Date'])
    print(f"\nResult:\n{df2}")
    df3=df2.loc['2006-06-01']
    print(f"\nResult:\n{df3}")

    monthTenor=df3["MonthTenor"]
    assert monthTenor==12
    #the monthly  reads don't remove some of the garbage
    #just check a  trade not in the file doesn't have data

    #check we have removed garbage
    for date_str in ['2006-04-22']:
        with pytest.raises(KeyError):
            bogus_data=df2.loc[date_str]
            print(f"\n{date_str}: Bogus data:\n{bogus_data}")
   
 