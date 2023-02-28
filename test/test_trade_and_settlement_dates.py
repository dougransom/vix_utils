import pytest
from vix_utils.vix_futures_term_structure import vix_futures_trade_dates_and_settlement_dates
import pandas as pd

trade_and_settle_df=pytest.fixture(vix_futures_trade_dates_and_settlement_dates)

def test_is_a_dataframe(trade_and_settle_df):
    assert isinstance(trade_and_settle_df,pd.DataFrame )

 