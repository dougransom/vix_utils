import pandas as pd
import pandas_market_calendars as mcal
import functools as func
import calendar as cal
import datetime as dt
import numpy as np
import vix_utils.futures_utils as u

import logging as logging
from .location import data_dir
_cfe_calendar = mcal.get_calendar('CFE')
_now = dt.datetime.now()

_years_ahead = 5
_future_date = dt.datetime(_now.year + _years_ahead, 1, 1)
# must be 1 year past _future_date
_last_valid_cfe_day = dt.datetime(_now.year + _years_ahead + 1, 1, 1)

# generate a range that is beyond any possible Expirys for vix futures in the data

_first_vix_futures_date_date = "2004-03-26"
#https://www.cboe.com/tradable_products/vix/vix_futures/specifications/

# don't use this for the date range.  _valid_cfe_days should go a year past the last trade date

_valid_cfe_days = pd.DatetimeIndex(
    _cfe_calendar.valid_days(_first_vix_futures_date_date, end_date=_last_valid_cfe_day).date).to_series().sort_index()


# days = _valid_cfe_days.date
# days1 = pd.DatetimeIndex(days).to_series()


@func.lru_cache(maxsize=None)  # called repeatedly with the same values, so cache the results.
def vix_futures_expiry_date_monthly(year: int, month: int):
    """
    Return the date of expiry of Vix Monthly Futures the series expiring in year and month
    :param year: The year of futures expiry
    :param month: The month of futures expiry
    :return:  The date of expiry of VIX Monthly Futures for the provided year and month.
    The specifications of expiry dates are in
    https://markets.cboe.com/tradeable_products/vix/vix_futures/specifications/.
    Holidays are taken into consideration.

    From the CFE Rulke book https://cdn.cboe.com/resources/regulation/rule_book/cfe-rule-book.pdf

        The final Expiry for a contract with the “VX” ticker symbol is on the Wednesday
    that is thirty days prior to the third Friday of the calendar month immediately following the
    month in which the contract expires. The final Expiry for a contract with the “VX”
    ticker symbol followed by a number denoting the specific week of a calendar year is on the
    Wednesday of the week specifically denoted in the ticker symbol. For symbology
    purposes, the first week of a calendar year is the first week of that year with a Wednesday
    on which a weekly VX futures contract could expire. If that Wednesday or the Friday that
    is thirty days following that Wednesday is a Cboe Options holiday, the final settlement
    date for the contract shall be on the business day immediately preceding that Wednesday.


    """
    c = cal.Calendar(firstweekday=cal.SUNDAY)
    next_month = month + 1

    # does the option series the future settles on  settle next year?
    options_next_year = next_month > 12

    next_month = next_month % 12 if options_next_year else next_month
    options_year = year + 1 if options_next_year else year  # keep for debugging

    m = c.monthdayscalendar(options_year, next_month)
    md = c.monthdatescalendar(options_year, next_month)

    friday_index = -2
    # 2 to index the 3d week, 0 based index for m
    week_index = 2 if m[0][friday_index] != 0 else 3
    # third_friday unused, just for easier debugging to have.
    third_friday = m[week_index][friday_index]              # keep for debugging
    option_expiry_date = md[week_index][friday_index]
    friday_expiration = any(_valid_cfe_days.isin([option_expiry_date]))
    if not friday_expiration:
        # the preceding day will be the option expiry date
        option_expiry_date = option_expiry_date - dt.timedelta(days=1)

    futures_expiry_date = option_expiry_date - dt.timedelta(days=30)
    # also check for a holiday on the 30 days before the 3d friday
    if friday_expiration and not any(_valid_cfe_days.isin([futures_expiry_date])):
        futures_expiry_date = futures_expiry_date - dt.timedelta(days=1)

    return futures_expiry_date


def vix_futures_expiry_date_from_trade_date(year, month, day, tenor):
    """
    :param year:  year of trade date
    :param month:  month of trade date
    :param day:    day of trade date
    :param tenor:   1 is the front  month, 2 the second, etc.
    :return:   VIX Futures Expiry
    """

    '''tenor is the number of months (or part months) to expiration.  the front month tenor is 1'''
    this_calendar_months_expiry = vix_futures_expiry_date_monthly(year, month)
    # deal with the part of the month, where the settlment month is the following month
    # if the month_of_settlement is less than the current month, then the  settlement is next year

    months_forward_for_tenor = (0 if day <=  this_calendar_months_expiry.day else 1)
    month_of_expiry = (month + months_forward_for_tenor + tenor - 1) % 12
    year_of_expiry = year + (
        1 if month_of_expiry < month else 0) + (tenor-1)//12
    return vix_futures_expiry_date_monthly(year_of_expiry, month_of_expiry)


def vix_constant_maturity_weights(vix_calendar):
    """
    :param vix_calendar:  the DataFrame returned by  vix_futures_trade_dates_and_expiry_dates
    :return: a DataFrame containting the weights required to interpolate between the tenors of trading tenors of
    Vix Futures to have a term structure of constant maturity in months.

    This index, for example,
    https://www.spglobal.com/spdji/en/indices/strategy/sp-500-vix-short-term-index-mcap/#overview,
    is based on a 1 month interpolation of the 1st and 2nd month futures.  The DataFrame returned should
    provide the weights required
    to calculate that index for any trade date.  Several ETFs are constructed from such indexes or weightings.

    You can read more at https://sixfigureinvesting.com/2015/01/how-does-vxx-daily-roll-work/.

    https://www.ipathetn.com/US/16/en/details.app?instrumentId=341408 for VXX information.

    SVIX the same way.  The periods are from the Tuesday prior from last months settlement date to
    the Tuesday before the current months settlement date.  in special cases it willbe the previous business day. 

    Here is roughly how it works.

    dt=number of business days in the current roll period
    dr=number of business days remaining in the current roll period
    front_month_weight=dr/dt
    next_month_weight=(1-front-month_weight)


    #the start of the roll period will be previous Expiry

    settlement_dates=vix_term_structure["Expiry"]
    vix_term_structure

    """

    # create a map from Expirys to the previous Expirys
    # this is done by looking at month 2 settlment dates, and finding the month 1 Expiry

    start_roll_front_month = "Start Roll Front Month"
    sd = "Expiry"
    rptd = "Roll Period Trade Days"
    rpcd = "Roll Period Calendar Days"
    settle_dates_map = vix_calendar[sd].drop_duplicates().dropna()
    month_to_prior_month_settlement_map = settle_dates_map.set_index(2)[1]
    cols_to_copy = {"Settle 1": vix_calendar['Expiry'][1], "Settle 2": vix_calendar['Expiry'][2]}
    df_foo = pd.DataFrame(index=vix_calendar.index, data=cols_to_copy)

    df_foo[rptd] = -100001  # just a nonsense number we can identify

    df_foo[rptd] = df_foo[rptd].astype(int)

    # add the start of roll date for front month

    df_foo[start_roll_front_month] = np.nan
    for second_month_settlement in month_to_prior_month_settlement_map.index:
        front_month_settlement=month_to_prior_month_settlement_map[second_month_settlement]
        start_roll = front_month_settlement
        selected = vix_calendar[sd][1] == front_month_settlement
        df_foo.loc[selected, start_roll_front_month] = start_roll
      
        calendar_day_day_before_settlement=second_month_settlement + pd.DateOffset(-1)

        #roll_period_trade_days is dt in https://www.spglobal.com/spdji/en/documents/methodologies/methodology-sp-vix-futures-indices.pdf page 5
        roll_period_trade_days = cfe_exchange_open_days(front_month_settlement, calendar_day_day_before_settlement) 
        df_foo.loc[selected, rptd] = roll_period_trade_days

    df_foo[start_roll_front_month] = pd.to_datetime(df_foo[start_roll_front_month])
    df_foo[rpcd] = vix_calendar[sd][1] - df_foo[start_roll_front_month]
    tenor_tds = "Tenor_Trade_Days"
    ten_caldays = "Tenor_Days"

    fmw = "Front Month Weight"
    smw = "Next Month Weight"
    trade_days_to_settle = df_foo[tenor_tds] = vix_calendar[tenor_tds][1]
    df_foo[fmw] = front_month_weight = trade_days_to_settle / df_foo[rptd]
    df_foo[smw] = -1 * front_month_weight + 1
    ttr = "Temp Trade Date"
    df_foo[ttr] = df_foo.index.to_series()
    #    temp_tdts="Temporary Tenor_Days"
    #    df_foo[temp_tdts]=df_foo[tdts]
    ll = len(df_foo)

    def maturity_date(row):
        # Use the trade date X trade days later, where X is the current roll period
        # in trade days.
        trade_date = row[ttr]
        try:
            row_roll_period_trade_days = row[rptd]  # trade_days_to_settle[trade_date]
            trade_date_loc = trade_days_to_settle.index.get_loc(trade_date)
            trade_date_loc_end_of_roll = trade_date_loc + row_roll_period_trade_days
            trade_date_loc_end_of_roll_capped = pd.nan if trade_date_loc_end_of_roll > ll else \
                trade_date_loc_end_of_roll
            trade_date_end_of_roll = df_foo.iloc[trade_date_loc_end_of_roll_capped].at[ttr]
            return trade_date_end_of_roll
        except Exception as e:
            pass
            #we are always going to get some of these at the end, since the dates go past the dates
            #in the data frames.  they need to be filtered out after.
            # print(f"Error {e} on row {row}")
        return pd.NaT

    constant_maturity_dates = df_foo.apply(maturity_date, axis=1, result_type='expand')
    #remove the NaTs.

    df_foo["Expiry"] = constant_maturity_dates
    #remove the NaTs.
    df_foo=df_foo[~constant_maturity_dates.isna()]

    df_foo.drop(ttr, axis=1, inplace=True)
    return df_foo


@u.timeit()
def pivot_on_contract_maturity(df):
    return df.reset_index().pivot(columns="Contract Month", index="Trade Date")


def cfe_exchange_open_days(start_date, end_date):
    exchange_open_days = _valid_cfe_days.loc[start_date:end_date]
    return len(exchange_open_days)

def remaining_cfe_exchange_open_days(start_date,end_date):
    next_day=start_date + pd.DateOffset(1)
    return cfe_exchange_open_days(next_day,end_date)

@u.timeit()
def vix_futures_trade_dates_and_expiry_dates(number_of_futures_maturities=9):
    f"""
    :param number_of_futures_maturities:
        :return:  a data frame with an index of trade date and maturity (in months) and a value of the Expiry.  
                   We refer to a DataFrame in this format as a wide vix calendar or wide settlement calendar..
                   The dates will include all past dates which the VIX futures have traded, and future dates until
                    {_future_date}
        """
    # by trial and error, this gives us the day
    # careful to select the portion of the valid dates before _future_date
    trade_dates = _valid_cfe_days[:_future_date]
    ii = pd.Index(trade_dates, name="Trade Date")

    @u.timeit()
    def add_columns_d(maturity):
        df = pd.DataFrame(index=ii)
        """Add the Contract Month (in months), Expiry, Tenor_Days, 
        and Tenor_Trade_Days to the dataframe"""
        df["Contract Month"] = maturity
        settle_columns = ['Expiry']

        for s in settle_columns:
            df[s] = np.nan

        def add_settle_date_and_trade_days_to_settlement(row):
            ix = row['tds']
            year, month, day = (ix.year, ix.month, ix.day)
            sd = vix_futures_expiry_date_from_trade_date(year, month, day, maturity)
            #use the date following the trade date to count the remaining days until 
            #expiry.


            tds = remaining_cfe_exchange_open_days(ix, sd)
            return sd, tds

        df['tds'] = df.index.to_series()  # need the index as values in the applied function
        new_cols = df.apply(add_settle_date_and_trade_days_to_settlement, axis=1, result_type='expand')
        df["Expiry"] = new_cols[0]
        df["Tenor_Trade_Days"] = new_cols[1]
        #df.drop('tds', axis=1,inplace=True)
        for s in settle_columns:
            df[s] = pd.to_datetime(df[s])
        df['Tenor_Days'] = (df['Expiry'] - df.index).dt.days.astype(np.int16)
        return df

    months = tuple(range(1, 1 + number_of_futures_maturities))
    # add in the Expiry and contract month columns
    settle_date_frames = (add_columns_d(m) for m in months)
    vix_all_months = u.timeit()(pd.concat)(settle_date_frames)

    # pivot the data so that it can be indexed by TradeDate and Contract Month
    cols = vix_all_months.columns
    unstacked = pivot_on_contract_maturity(vix_all_months)
    # print(f"unstacked: \{unstacked}")
    return unstacked


 
    