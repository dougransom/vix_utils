"""
A library for preparing   VIX Futures and Cash Term Structures for analysis,
including a  continuous maturity VIX Futures term structure.
"""
__version__ = '0.1.6'

from .download_vix_futures import  \
pivot_futures_on_monthly_tenor,select_monthly_futures,async_load_vix_term_structure,load_vix_term_structure

from .vix_cash_term_structure import \
    async_get_vix_index_histories,  \
    get_vix_index_histories,    \
    pivot_spot_term_structure_on_symbol

from .vix_futures_dates import vix_futures_expiry_date_monthly, \
    vix_futures_expiry_date_from_trade_date, \
    vix_futures_trade_dates_and_expiry_dates, \
    vix_constant_maturity_weights   

from .continuous_maturity import continuous_maturity_one_month,append_continuous_maturity_one_month

