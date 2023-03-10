import pytest
import typing
from vix_utils.vix_futures_dates import  vix_futures_expiry_date_from_trade_date
import datetime as dt
from  functools import partial
from . import fiso
 
#the settlement_date_str_list is by tenor
@pytest.mark.parametrize("date_yy_mm_dd,settlement_date_str_list",\
    [ ((2013,1,15),[ '2013-01-16', '2013-02-13','2013-03-20', '2013-04-17', '2013-05-22', '2013-06-19', '2013-07-17', '2013-08-21']),
     ((2013,1,17),[  '2013-02-13','2013-03-20', '2013-04-17', '2013-05-22', '2013-06-19', '2013-07-17', '2013-08-21']),
     ((2016,12,21),['2016-12-21', '2017-01-18', '2017-02-15', '2017-03-22', '2017-04-19', '2017-05-17', '2017-06-21', '2017-07-19', '2017-08-16']),
     ((2006,3,30), ['2006-04-19']),
     ((2006,6,1), ['2006-06-21','2006-07-19','2006-08-16', '2006-09-20','2006-10-18','2006-11-15','2006-12-20',
    '2007-01-17','2007-02-14','2007-03-21',   '2007-04-18','2007-05-16','2007-06-20']),
    ((2013,1,16),[ '2013-01-16', '2013-02-13','2013-03-20', '2013-04-17', '2013-05-22', '2013-06-19', '2013-07-17', '2013-08-21']) ] )      

def test_settlement_from_trade_date(date_yy_mm_dd,settlement_date_str_list):
 

    settlements_from_tenor=partial(vix_futures_expiry_date_from_trade_date,*date_yy_mm_dd)

    dates = [fiso(strdate) for strdate in settlement_date_str_list]
    for tenor, expected in zip(range(1,1+len(dates)),dates):
        settlement=settlements_from_tenor(tenor)
        print(f"tenor {tenor} settlement {settlement} expected {expected}")
        assert(settlement==expected)
        
@pytest.mark.parametrize("trade_date_str,tenor, settlement_date_str",
    [('2006-06-01',1,'2006-06-21'),('2006-06-01',12,'2007-05-16')])
def test_settlement_from_trade_date_tenor(trade_date_str,tenor,settlement_date_str):
    td=fiso(trade_date_str)
    expected_sd=fiso(settlement_date_str)
    settlement=vix_futures_expiry_date_from_trade_date(td.year,td.month,td.day,tenor)
    print(f"tenor {tenor} settlement {settlement} expected {expected_sd}")

    assert settlement==expected_sd


    

   