import pytest
import typing
from vix_utils.vix_futures_term_structure import  vix_futures_settlement_date_from_trade_date
import datetime as dt
from  functools import partial

 
fiso=dt.date.fromisoformat

def test_jan_15_2013():
    yy=2013
    mm=1
    dd=15

    settlements_from_tenor=partial(vix_futures_settlement_date_from_trade_date,yy,mm,dd)

    dates = [fiso(strdate) for strdate in [ '2013-01-16', '2013-02-13','2013-03-20', '2013-04-17', '2013-05-22', '2013-06-19', '2013-07-17', '2013-08-21']]
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
  

