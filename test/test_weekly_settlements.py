import pytest
import typing
from vix_utils.download_vix_futures import CBOFuturesDates
from vix_utils.download_vix_futures import vix_settlements
 
import datetime as dt
from  functools import partial
fiso=dt.date.fromisoformat

@pytest.fixture
def weekly_settlements_set():
        return frozenset(vix_settlements(2000,2030))

settlement_test_dates_wed_tuesday= [
                        ('2020-01-08','2020-01-07'),
                        ('2020-01-15','2020-01-14'),
                        ('2020-01-22','2020-01-21'),
                        ('2020-01-29','2020-01-28'),
                        ('2020-09-23','2020-09-22'),
                        ('2022-03-02','2022-03-01'),
                        ('2022-03-09','2022-03-08'),
                        ('2022-03-16','2022-03-15'),  #monthly
                        ('2022-03-23','2022-03-22'),
                        ('2022-03-30','2022-03-29'),
                        ('2020-12-02','2020-12-01'),
                        ('2019-01-09','2019-01-08'),
                        ('2019-11-06','2019-11-05'),
                        ('2019-11-19','2019-11-19'),
                        ('2019-11-27','2019-11-26'),
                        ('2019-12-25','2019-12-24'),
                        ]

a,b=zip(*settlement_test_dates_wed_tuesday)
settlement_test_dates=a+b

@pytest.mark.parametrize("settlement_test_date",settlement_test_dates)
def test_settlements(settlement_test_date,weekly_settlements_set):
    d=dt.date.fromisoformat(settlement_test_date)
    assert(d in weekly_settlements_set)


 
def test_not_settlements(weekly_settlements_set):
    d=dt.date.fromisoformat('2019-12-26')
    assert (not d in weekly_settlements_set) 


if __name__=="__main__":
    # for week in range(1,54):
    #     d=c.vix_settlement_date_weekly(2019,week)
    #     print(f"week {week}  dates {d}")
    s=list(vix_settlements(2019,2020))
    print(f"Settlements {s}")

