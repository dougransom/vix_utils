import pytest

from vix_utils  import vix_futures_expiry_date_monthly

from . import fiso


settlment_test_dates= [((2016,12),'2016-12-21'),  ((2021,1),'2021-01-20') , ((2013,12),'2013-12-18'),
                       ((2013,1),'2013-01-16')]

@pytest.mark.parametrize("yy_mm,expected_date_str",settlment_test_dates)
def test_settlements(yy_mm,expected_date_str):
    expected_date=fiso(expected_date_str)
    
    computed_date=vix_futures_expiry_date_monthly(*yy_mm)
    assert(computed_date==expected_date)
