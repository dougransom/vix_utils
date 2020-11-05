

import pandas as pd
import pandas_market_calendars as mcal
import functools as func
from itertools import islice,tee
import datetime as dt

import calendar as cal
import datetime as dt
import pandas_market_calendars as mcal
import numpy as np

import quandl as ql


cache_file="C:/Users/dougr/OneDrive/family/doug/work in progress/python projects/m1m2/vx_close.pyarrow"
#pd.set_option('display.max_rows', 75)
#pd.set_option('display.min_rows', 75)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

_cfe_calendar =  mcal.get_calendar('CFE')
_now = dt.datetime.now()
_five_years_away = dt.datetime(_now.year + 6, 1, 1)
#generate a range that is beyond any possible settlment dates for vix futures in the data
_valid_cfe_days = _cfe_calendar.valid_days(start_date='2000-12-20', end_date=_five_years_away).to_series();



@func.lru_cache(maxsize=None)       #called repeatedly with the same values, so cache the results.
def vix_futures_settlement_date_monthly( year, month):
    """
    Return the date of expiry of Vix Monthly Futures the series expiring in year and month
    :param year: The year of futures expiry
    :param month: The month of futures expiry
    :return:  The date of expiry of VIX Monthly Futures for the provided year and month.
    The specifications of expiry dates are in https://markets.cboe.com/tradeable_products/vix/vix_futures/specifications/.
    Holidays are taken into consideration.

    """
    c = cal.Calendar(firstweekday=cal.SUNDAY)
    next_month = month + 1

    # does the option series the future settles on  settle next year?
    options_next_year = next_month > 12

    next_month = next_month % 12 if options_next_year else next_month
    options_year = year + 1 if options_next_year else year

    m = c.monthdayscalendar(year, next_month)
    md = c.monthdatescalendar(year, next_month)

    friday_index = -2
    # 2 to index the 3d week, 0 based index for m
    week_index = 2 if m[0][friday_index] != 0 else 3
    # third_friday unused, just for easier debugging to have.
    third_friday = m[week_index][friday_index]
    option_expiry_date = md[week_index][friday_index]
    friday_expiration = any(_valid_cfe_days.isin([option_expiry_date]))
    if not friday_expiration:
        #the preceeding day will be the option expiry date
        option_expiry_date = option_expiry_date-dt.timedelta(days=1)

    futures_expiry_date = option_expiry_date - dt.timedelta(days=30)
    #also check for a holiday on the 30 days before the 3d friday
    if  friday_expiration and not any(_valid_cfe_days.isin([option_expiry_date])):
        futures_expiry_date=futures_expiry_date-dt.timedelta(days=1)

    return futures_expiry_date

def vix_futures_settlement_date_from_trade_date(year,month,day,tenor):
    '''tenor is the number of months (or part months) to expiration.  the front month tenor is 1'''
    this_calendar_months_settlement=vix_futures_settlement_date_monthly(year,month)
    #deal with the part of the month, where the settlment month is the following month
    months_forward_for_tenor =   (0 if  day < this_calendar_months_settlement.day   else 1)
    month_of_settlement = (month+months_forward_for_tenor+tenor -1 )%12
    year_of_settlement = year + (1 if month_of_settlement < month else 0)   #if the month_of_settlment is less than the current month, then the  settlment is next year
    return vix_futures_settlement_date_monthly(year_of_settlement,month_of_settlement)

def vix_futures_term_structure(quandl_api_key,number_of_futures_maturities=3):
    """Download the futures data from quandl for the month 1,...number_of_futures_maturities.
    Add columns for the settlement date, number of days until settlement, and number of trade days to settlement.
    """


    def add_columns(df, maturity):
        """Add the Tenor, Settlement Date, Trade Days to Settlement, and Days to Settlement to the dataframe"""
        df["Contract Month"] =  maturity
        df["Settlement Date"]=np.nan
        settlement_date = df['Settlement Date']
        #these seems a little brute force, but it is fast enough.
        for ix in  df.index:
            year,month,day = (ix.year,ix.month,ix.day)
            sd = vix_futures_settlement_date_from_trade_date(year, month, day, maturity)
            df.loc[ix,"Settlement Date"] = sd
            exchange_open_days = _valid_cfe_days.loc[ix:sd]
            trade_days_to_settlement = len(exchange_open_days)
            df.loc[ix,"Trade Days to Settlement"]=trade_days_to_settlement

        df["Settlement Date"]=pd.to_datetime(df["Settlement Date"])
        df['Days to Settlement'] = ((df['Settlement Date'] - df.index).dt.days).astype(np.int16)

        return df

    months = tuple(range(1, 1 + number_of_futures_maturities))
    ql.ApiConfig.api_key=quandl_api_key
    #the quandle query strings
    qc = list((f"CHRIS/CBOE_VX{i}" for i in months))
    #the data frame for each future month (1m, 2m etc.) from quandl
    zmvix =zip(months,(ql.get(a) for a in qc))
    #add in the settlment date and tenor columnss
    zmvix1= (  add_columns(df,m)           for m,df in zmvix)
    #bring them all together into a dataframe
    vix_all_months=  pd.concat(zmvix1)

    #pivot the data so that it can be indexed by TradeDate and Tenor
    print(f"Vix all months {vix_all_months}")
    print(f"\nVix All Months Close Columns {vix_all_months.columns}\n ")
    cols = vix_all_months.columns
    stacked = vix_all_months.reset_index().pivot(columns="Contract Month",index="Trade Date")

    return stacked

    settlement_dates = vix_all_months['Settlement Date'].unique()
    #print(f"Settlement Dates {settlement_dates}")



