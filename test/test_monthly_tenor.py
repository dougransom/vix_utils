import pytest
import typing
from vix_utils.vix_futures_dates import  vix_futures_settlement_date_from_trade_date
import datetime as dt
from  functools import partial
from . import fiso
 

@pytest.mark.parametrize("date_yy_mm_dd,trade_date_str_list",\
    [ ((2013,1,15),[ '2013-01-16', '2013-02-13','2013-03-20', '2013-04-17', '2013-05-22', '2013-06-19', '2013-07-17', '2013-08-21']),
    ((2013,1,16),[ '2013-01-16', '2013-02-13','2013-03-20', '2013-04-17', '2013-05-22', '2013-06-19', '2013-07-17', '2013-08-21']) ] )      

def test_settlement_from_trade_date(date_yy_mm_dd,trade_date_str_list):
 

    settlements_from_tenor=partial(vix_futures_settlement_date_from_trade_date,*date_yy_mm_dd)

    dates = [fiso(strdate) for strdate in trade_date_str_list]
    for tenor, expected in zip(range(1,1+len(dates)),dates):
        settlement=settlements_from_tenor(tenor)
        assert(settlement==expected)
        print(f"tenor {tenor} settlment {settlement} expected {expected}")

def test_jan_16_2013():
    yy=2013
    mm=1
    dd=16

    settlements_from_tenor=partial(vix_futures_settlement_date_from_trade_date,yy,mm,dd)

    dates = [fiso(strdate) for strdate in [ '2013-01-16', '2013-02-13','2013-03-20', '2013-04-17', '2013-05-22', '2013-06-19', '2013-07-17', '2013-08-21']]
    for tenor, expected in zip(range(1,1+len(dates)),dates):
        settlement=settlements_from_tenor(tenor)
        assert(settlement==expected)
        print(f"tenor {tenor} settlment {settlement} expected {expected}")

    
def test_jan_17_2013():
    yy=2013
    mm=1
    dd=17

    settlements_from_tenor=partial(vix_futures_settlement_date_from_trade_date,yy,mm,dd)

    dates = [fiso(strdate) for strdate in [  '2013-02-13','2013-03-20', '2013-04-17', '2013-05-22', '2013-06-19', '2013-07-17', '2013-08-21']]
    for tenor, expected in zip(range(1,1+len(dates)),dates):
        settlement=settlements_from_tenor(tenor)
        assert(settlement==expected)
        print(f"tenor {tenor} settlment {settlement} expected {expected}")
  

def test_dec_21_2016():
    yy=2016
    mm=12
    dd=21
    settlements_from_tenor=partial(vix_futures_settlement_date_from_trade_date,yy,mm,dd)
    dates_str=['2016-12-21', '2017-01-18', '2017-02-15', '2017-03-22', '2017-04-19', '2017-05-17', '2017-06-21', '2017-07-19', '2017-08-16']
    dates=[fiso(strdate) for strdate in dates_str]
    for tenor, expected in zip(range(1,1+len(dates)),dates):
        settlement=settlements_from_tenor(tenor)
        assert(settlement==expected)
        print(f"tenor {tenor} settlment {settlement} expected {expected}")
 
