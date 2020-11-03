import pandas_market_calendars as mcal
import datetime as dt
import calendar as cal
import pandas as pd
_now=dt.datetime.now()
_cfe_mcal = mcal.get_calendar('CFE')
_five_years_away=dt.datetime(_now.year+6,1,1)
_valid_days=_cfe_mcal.valid_days(start_date='2000-12-20', end_date=_five_years_away)
_cfe_start_year=2000
_cfe_stop_year=_five_years_away.year
# get info for futures expiring up to January 1 in six years.
# no futures currently trade that far out so this should be fine

class CBOEFuturesSettlementDates:
    def __init__(self):
        pass


def _years_months():
    '''
    :param self:
    :return: Years and month tuples from 2000 until  between 5 and 6years from now
    '''
    for year in range(_cfe_start_year, _cfe_stop_year):
        for month in range(1, 12):
            yield (year, month)

class VixFuturesSettlmentDates(CBOEFuturesSettlementDates):
        def __init__(self):
            CBOEFuturesSettlementDates.__init__(self)

        def get_settlement_dates(self):
            di = pd.DatetimeIndex(self.get_settlement_date(y,m) for (y,m) in _years_months())
            return di


        # http://www.cboe.com/products/futures/vx-cboe-volatility-index-vix-futures/contract-specifications
        # Final Settlement Date:
        # The final settlement date for a contract with the "VX" ticker symbol is on the Wednesday that is 30 days
        #  prior to the third Friday of the calendar month immediately following
        #  the month in which the contract expires.
        # The final settlement date for a futures contract with
        # the "VX" ticker symbol followed by a number denoting the specific week of a calendar
        # year is on the Wednesday of the week specifically denoted in the ticker
        # symbol.

        # If that Wednesday or the Friday that is 30 days following that Wednesday is a Cboe Options holiday,
        # the final settlement date for the contract shall be on the business day immediately preceding that Wednesday.
        # VX Futures Symbols - VX* and VX01 through VX53**. Embedded numbers denote the
        # specific week of a calendar year during which a contract is settled. For
        #  symbology purposes, the first week of a calendar year is the first
        #  week of that year
        # with a Wednesday on which a weekly VX futures contract could expire.

        def get_settlement_date(self, year, month, week_number=0):
            c = cal.Calendar(cal.SUNDAY)

            m = c.monthdayscalendar(year, 1)
            md = c.monthdatescalendar(year, 1)
            # find the first wednesday of the year
            wednesday_index = 3
            wednesday_in_first_week = m[0][wednesday_index] != 0
            first_wednesday = md[0 if wednesday_in_first_week else 1][wednesday_index]
            settlement_date = first_wednesday + dt.timedelta(weeks=(week_number - 1))
            options_settlement_date = settlement_date+dt.timedelta(days=30)
            mask=_valid_days.isin([options_settlement_date,settlement_date])
            either_on_holiday=any(mask)



            #        print(f"Weekly {year} {month} {week_number}  settlement date: {settlement_date}")

            return settlement_date