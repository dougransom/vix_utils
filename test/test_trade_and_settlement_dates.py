import pytest
from vix_utils.vix_futures_dates import vix_futures_trade_dates_and_expiry_dates
import pandas as pd

trade_and_settle_df=pytest.fixture(vix_futures_trade_dates_and_expiry_dates)

def test_is_a_dataframe(trade_and_settle_df):
    assert isinstance(trade_and_settle_df,pd.DataFrame )

 