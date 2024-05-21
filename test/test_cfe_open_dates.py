import pytest
import vix_utils as vu
from vix_utils.vix_futures_dates import cfe_exchange_open_dates
import pandas as pd

def test_may_2024_cfe_open():
    may_dates : pd.DatetimeIndex=cfe_exchange_open_dates(slice('2024-05','2024-05'))
    #exchange holiday
    with pytest.raises(KeyError):
        may_dates.loc['2024-05-27']
    may_dates.loc['2024-05-28']        #known to be an open day, should not raise

    open_day_count :int = len(may_dates)
    assert open_day_count == 22, "22 trading days in may 2024 (https://cdn.cboe.com/resources/aboutcboe/Cboe-2024FuturesSettlementCalendar.pdf) "

    print(may_dates)


def test_week_of_march_24_2024():
    week_start='2024-03-24'
    week_end='2024-03-30'
    expected_open_days=['2024-03-25','2024-03-26','2024-03-27','2024-03-28']
    open_dates_that_week : pd.DatetimeIndex = cfe_exchange_open_dates(slice(week_start,week_end))
    assert len(open_dates_that_week)==4, f"4 trading days in the week starting {week_start}"
    for ed in expected_open_days:
        open_dates_that_week.loc[ed]        #make sure they are alll there.
