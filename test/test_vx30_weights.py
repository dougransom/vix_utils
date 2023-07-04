import pytest
import typing
from vix_utils.vix_futures_dates import  *
import datetime as dt
from  functools import partial
from . import fiso

vix_dates = vix_futures_trade_dates_and_expiry_dates(3)

