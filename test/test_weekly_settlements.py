import pytest
import typing
from vix_utils.download_vix_futures import CBOFuturesDates

 
import datetime as dt
from  functools import partial
fiso=dt.date.fromisoformat

c=CBOFuturesDates()

settlement_test_dates= [((2020,39),('2020-09-23','2020-09-22')),
                        ((2022,9), ('2022-03-02','2022-03-01')),
                        ((2022,10), ('2022-03-09','2022-03-08')),
                        ((2022,11), ('2022-03-16','2022-03-15')),  #monthly
                        ((2022,12), ('2022-03-23','2022-03-22')),
                        ((2022,13), ('2022-03-30','2022-03-29')),
                        ((2020,48), ('2020-11-25','2020-11-24')),
                        ((2019,48),('2019-11-27','2019-11-26')),
                        ]

@pytest.mark.parametrize("yy_week,expected_date_str_tuple",settlement_test_dates)
def test_settlements(yy_week,expected_date_str_tuple):
    expected_dates=(fiso(d) for d in expected_date_str_tuple)
    computed_dates=c.vix_settlement_date_weekly(*yy_week)
    
    for ed,cd in zip(expected_dates,computed_dates):
        assert(ed==cd)
