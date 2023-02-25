import pytest
import typing
from vix_utils.download_vix_futures import CBOFuturesDates

 
import datetime as dt
from  functools import partial
fiso=dt.date.fromisoformat

c=CBOFuturesDates()

settlement_test_dates= [((2020,39),'2020-09-23'),
                        ((2022,9), '2022-03-02'),
                        ((2022,10), '2022-03-09'),
                        ((2022,11), '2022-03-15'),  #monthly
                        ((2022,12), '2022-03-23'),
                        ((2022,13), '2022-03-30'),

                        ]

@pytest.mark.parametrize("yy_week,expected_date_str",settlement_test_dates)
def test_settlements(yy_week,expected_date_str):
    expected_date=fiso(expected_date_str)
    
    computed_date=c.vix_settlement_date_weekly(*yy_week)
    assert(computed_date==expected_date)
