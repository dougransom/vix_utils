

import pandas as pd
import pandas_market_calendars as mcal
import functools as func
from itertools import islice,tee
import datetime as dt

import calendar as cal
import datetime as dt
import pandas_market_calendars as mcal
import numpy as np
import utils.futures_utils as u
import quandl as ql

import logging as logging


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

def vix_1m_term_structure(vix_term_structure):
    """
    dt=number of business days in the current roll period
    dr=number of business days remaining in the current roll period
    front_month_weight=dr/dt
    next_month_weight=(1-front-month_weight)


    #the start of the roll period will be previous settlement date

    settlement_dates=vix_term_structure["Settlement Date"]
    vix_term_structure

    https://www.spglobal.com/spdji/en/indices/strategy/sp-500-vix-short-term-index-mcap/#overview

    :param durations_in_days:
    :param vix_term_structure:
    :return:
    """

    #create a map from settlment dates to the previous settlement dates
    #this is done by looking at month 2 settlment dates, and finding the month 1 settlement date

    srfm="Start Roll Front Month"
    sd="Settlement Date"
    rptd="Roll Period Trade Days"
    rpcd="Roll Period Calendar Days"
    settle_dates_map=vix_term_structure[sd].drop_duplicates().dropna()
    second_month_to_front_month=settle_dates_map.set_index(2)[1]

    #add the start of roll date for front month
    vix_term_structure[srfm]=np.nan
    for ix in second_month_to_front_month.index:
        start_roll = second_month_to_front_month[ix]
        selected = vix_term_structure[sd][1]==ix
        vix_term_structure.loc[selected,srfm]=start_roll
        roll_period_trade_days = cfe_exchange_open_days(start_roll,ix)
        vix_term_structure.loc[selected,rptd]=roll_period_trade_days

    vix_term_structure[srfm]=pd.to_datetime(vix_term_structure[srfm])
    print(f"\nsd\n{vix_term_structure[sd]}\nsrfm\n{vix_term_structure[srfm]}")
    vix_term_structure[rpcd]=vix_term_structure[sd][1]-vix_term_structure[srfm]
    cdts="Days to Settlement"
    tdts="Trade Days to Settlement"

    spxvstr_w="SPVIXSTR Front Month Weight"
    v1="VIX1M_SPVIXSTR"
    vix_term_structure[spxvstr_w]=vix_term_structure[tdts][1]/vix_term_structure[rptd]
    weight_second_month = -1*vix_term_structure[spxvstr_w]+1
    print(f"Weight second month \n{weight_second_month}")
    front_contribution=vix_term_structure["Close"][1]*vix_term_structure[spxvstr_w]
    second_contribution=weight_second_month*vix_term_structure["Close"][2]

    #vix_term_structure[v1]=vix_term_structure["Close"][1]*vix_term_structure[spxvstr_w] + (vix_term_structure[spxvstr_w]-1)*-1
    vix_term_structure[v1]=front_contribution+second_contribution
    vtemp=vix_term_structure[["Close",sd,srfm,tdts,cdts,rptd,rpcd,spxvstr_w,v1]][-50:]

    print(f"\n Term Structure with foo  {vtemp}")

    return vix_term_structure




@u.timeit()
def pivot_on_contract_maturity(df):
    return df.reset_index().pivot(columns="Contract Month", index="Trade Date")

def cfe_exchange_open_days(start_date,end_date):
    exchange_open_days = _valid_cfe_days.loc[start_date:end_date]
    return  len(exchange_open_days)



def vix_futures_term_structure(quandl_api_key,number_of_futures_maturities=3):
    """Download the futures data from quandl for the month 1,...number_of_futures_maturities.
    Add columns for the settlement date, number of days until settlement, and number of trade days to settlement.
    """

    def add_columns(df, maturity):
        """Add the Tenor, Settlement Date, Trade Days to Settlement, and Days to Settlement to the dataframe"""
        df["Contract Month"] =  maturity
#        settle_columns = ['Settlement Date','Previous Settlement Date']
        settle_columns = ['Settlement Date']

        for s in settle_columns:
            df[s]=np.nan

        settlement_date = df['Settlement Date']
        #these seems a little brute force, but it is fast enough.
        for ix in  df.index:
            year,month,day = (ix.year,ix.month,ix.day)
            sd = vix_futures_settlement_date_from_trade_date(year, month, day, maturity)
            df.loc[ix,"Settlement Date"] = sd
            df.loc[ix,"Trade Days to Settlement"] = cfe_exchange_open_days(ix,sd)
        for s in settle_columns:
            df[s]=pd.to_datetime(df[s])
        df['Days to Settlement'] = ((df['Settlement Date'] - df.index).dt.days).astype(np.int16)

        return df

    months = tuple(range(1, 1 + number_of_futures_maturities))
    ql.ApiConfig.api_key=quandl_api_key
    #the quandle query strings
    qc = list((f"CHRIS/CBOE_VX{i}" for i in months))
    #the data frame for each future month (1m, 2m etc.) from quandl
    method=u.timeit()(ql.get)

    zmvix =zip(months,(method(a) for a in qc))
    #add in the settlment date and tenor columnss
    zmvix1= list(  add_columns(df,m)           for m,df in zmvix)
    #bring them all together into a dataframe
    vix_all_months=  u.timeit()(pd.concat)(zmvix1)

    #pivot the data so that it can be indexed by TradeDate and Tenor
    print(f"Vix all months {vix_all_months}")
    print(f"\nVix All Months Close Columns {vix_all_months.columns}\n ")
    cols = vix_all_months.columns
    unstacked = pivot_on_contract_maturity(vix_all_months)


    return unstacked




