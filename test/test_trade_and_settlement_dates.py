import pytest
from vix_utils import vix_futures_trade_dates_and_expiry_dates,vix_constant_maturity_weights
from vix_utils.vix_futures_dates import remaining_cfe_exchange_open_days
import pandas as pd

trade_and_settle_df=pytest.fixture(vix_futures_trade_dates_and_expiry_dates)
ts_2023=pytest.fixture(lambda:vix_futures_trade_dates_and_expiry_dates(2).loc['2023'])

@pytest.fixture()
def weights_2023(trade_and_settle_df):
    w=vix_constant_maturity_weights(trade_and_settle_df).loc['2023']
    return w

def test_is_a_dataframe_1(trade_and_settle_df):
    assert isinstance(trade_and_settle_df,pd.DataFrame )

def test_is_a_dataframe_2(ts_2023,weights_2023: None):
    assert isinstance(ts_2023,pd.DataFrame )
     
 
def test_output_info(ts_2023,weights_2023):
    """Just output the dataframes for inspection"""
    with pd.option_context('display.max_rows',None,'display.max_columns',None):
        print(f"\nts_2023_06:\n{ts_2023}")
        print(f"\nts_2023_06 cols:\n {ts_2023.columns}")
        print(f"\nWeights:\n{weights_2023}")

#from https://cdn.cboe.com/resources/aboutcboe/Cboe-2023FuturesSettlementCalendar.pdf
#June expiry June 21, July expiry, July 19, August Expiry August 16
#Also, remember we are using the dates for weights at the end of the trading day on the date.



#as of 20230704   the June 19 holiday is handled incorrectly by pandas_market_calculators,
#so those tests fail and our trade days to expiry could be off by one.
#shouldn't affect analysis too much.

trade_days_data=[ ('2023-06-01',13,33),
                 ('2023-06-16', 2,22),
                ('2023-03-31',12,32)]  
cal_days_data=[ ('2023-06-01',20,48)]

trade_date_data=[ ('2023-06-16','2023-06-21',2),
                  ('2023-06-01','2023-06-21',13),
                  ('2023-04-30','2023-05-17',13),
                  ('2023-03-31','2023-04-19',12),
                  ('2023-03-31','2023-05-17',32)]

               
@pytest.mark.parametrize("Trade_Date,Tenor_Days_1,Tenor_Days_2",cal_days_data)
def test_calendar_days_to_expiry(ts_2023,Trade_Date,Tenor_Days_1,Tenor_Days_2):
    with pd.option_context('display.max_rows',None,'display.max_columns',None):
        row=ts_2023.loc[Trade_Date]
        print(f"\nRow\n{row}")    
        assert(Tenor_Days_1 == row['Tenor_Days',1])
        assert(Tenor_Days_2 == row['Tenor_Days',2])



@pytest.mark.parametrize("Trade_Date,Tenor_Trade_Days_1,Tenor_Trade_Days_2",trade_days_data)
def test_trade_days_to_expiry(ts_2023,Trade_Date,Tenor_Trade_Days_1,Tenor_Trade_Days_2):
    with pd.option_context('display.max_rows',None,'display.max_columns',None):
        row=ts_2023.loc[Trade_Date]
        print(f"\nRow\n{row}")    
        assert(Tenor_Trade_Days_1 == row['Tenor_Trade_Days',1])
        assert(Tenor_Trade_Days_2 == row['Tenor_Trade_Days',2])

@pytest.mark.parametrize("trade_date,expiry_date,remaining",trade_date_data)               
def test_remaining_trade_days(trade_date,expiry_date,remaining):
    pd_dates=[pd.to_datetime(x) for x in(trade_date,expiry_date)]
    assert(remaining == remaining_cfe_exchange_open_days(*pd_dates) )

