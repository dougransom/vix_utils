import pytest
import vix_utils as vu
from vix_utils.vix_futures_dates import vix_constant_maturity_weights
import pandas as pd
pd.set_option('display.max_rows',5)
pd.set_option('display.max_columns',None)

#vix_dates=vu.vix_futures_dates.vix_futures_trade_dates_and_expiry_dates()

#weights=vix_constant_maturity_weights(vix_dates)

#abc=weights.loc["2024-04-15" : "2024-04-30"]
#print(f"weights:\n{abc}\n")

@pytest.fixture 
def vix_dates():
    return vu.vix_futures_dates.vix_futures_trade_dates_and_expiry_dates()

@pytest.fixture
def m1m2_weights(vix_dates):
    return vix_constant_maturity_weights(vix_dates)

@pytest.mark.parametrize("column",["Front Month Weight","Next Month Weight"])
def test_weights_in_range(m1m2_weights : pd.DataFrame,column):
    ''' Verify the weights of two futures to produce a weighted maturity are in the range [0,1]'''

    #note that with floating point it is possible one day to experince rounding errors that would make a weight a hair outside of 
    #the range [0,1] in which case this test might have to be tweaked.
    negative_weights_sel : pd.DataFrame = m1m2_weights[column] < 0
    weights_gt_1_sel : pd.DataFrame =  m1m2_weights[column] > 1.0


    weights_out_of_range=m1m2_weights[negative_weights_sel|weights_gt_1_sel]
    
    with pd.option_context('display.max_rows',None,'display.max_columns',None):
        if not weights_out_of_range.empty:
             print(f"test_weights_in_range{column}\n:{weights_out_of_range}")

    assert weights_out_of_range.empty, f"{column} should be between 0 and 1:  {weights_out_of_range}"

def test_weights_sum_to_one(m1m2_weights : pd.DataFrame):
    ''' Verify the weights of two futures to produce a weighted maturity sum to one'''
    
    
    #note that with floating point it is possible one day to experince rounding errors that would make a weight a hair outside of 
    #the range [0,1] in which case this test might have to be tweaked.
    weights_sum = m1m2_weights['Front Month Weight'] + m1m2_weights['Next Month Weight']
    rows_not_summing_to_one = m1m2_weights[ weights_sum !=  1.0]
    count_rows=rows_not_summing_to_one.shape[0]
    with pd.option_context('display.max_rows',None,'display.max_columns',None):
        assert rows_not_summing_to_one.empty, f"The sum of weights of two futures should be one.  {count_rows} instances of failure:\n{rows_not_summing_to_one} "
