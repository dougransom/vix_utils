quandl_api_key="5cDGQqduzQgmM_2zfkd1"
months_vx_available=3
import pandas as pd
import pandas_market_calendars as mcal
import functools as func
from itertools import islice,tee
import datetime as dt

import calendar as cal
import datetime as dt
import pandas_market_calendars as mcal
import numpy as np

download_vix_futures_data_required=True
months = tuple(range(1, 1 + months_vx_available))
qc = list((f"CHRIS/CBOE_VX{i}" for i in months))
cache_file="C:/Users/dougr/OneDrive/family/doug/work in progress/python projects/m1m2/vx_close.pyarrow"
#pd.set_option('display.max_rows', 75)
#pd.set_option('display.min_rows', 75)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

cfe =  mcal.get_calendar('CFE')
now = dt.datetime.now()
five_years_away = dt.datetime(now.year + 6, 1, 1)
#generate a range that is beyond any possible settlment dates for vix futures in the data
valid_days = cfe.valid_days(start_date='2000-12-20', end_date=five_years_away).to_series();


#https://markets.cboe.com/tradeable_products/vix/vix_futures/specifications/
#for the algorithm to determine settlment date of futures including
#holiday adjustments.
#we cache the results as we call this repeatedly with same parameters.

@func.lru_cache(maxsize=None)
def vix_futures_settlement_date_monthly( year, month):
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
    friday_expiration = any(valid_days.isin([option_expiry_date]))
    if not friday_expiration:
        #the preceeding day will be the option expiry date
        option_expiry_date = option_expiry_date-dt.timedelta(days=1)

    futures_expiry_date = option_expiry_date - dt.timedelta(days=30)
    #also check for a holiday on the 30 days before the 3d friday
    if  friday_expiration and not any(valid_days.isin([option_expiry_date])):
        futures_expiry_date=futures_expiry_date-dt.timedelta(days=1)

    # no knowns special cases for monthly vix futures settlement dates as of 2020-08-15
    #        print(f"Year {year} month {month} options {option_expiry_date} futures {futures_expiry_date}  ")
    return futures_expiry_date

def vix_futures_settlement_date_from_trade_date(year,month,day,tenor):
    '''tenor is the number of months (or part months) to expiration.  the front month tenor is 1'''
    this_calendar_months_settlement=vix_futures_settlement_date_monthly(year,month)
    #deal with the part of the month, where the settlment month is the following month
    months_forward_for_tenor =   (0 if  day < this_calendar_months_settlement.day   else 1)
    month_of_settlement = (month+months_forward_for_tenor+tenor -1 )%12
    year_of_settlement = year + (1 if month_of_settlement < month else 0)   #if the month_of_settlment is less than the current month, then the  settlment is next year
    return vix_futures_settlement_date_monthly(year_of_settlement,month_of_settlement)


if download_vix_futures_data_required:
    import quandl as ql
    ql.ApiConfig.api_key=quandl_api_key
    #the data frame for each future month (1m, 2m etc.)
    #we need two iterators so we can initialize a dataframe with the index of the same type
    #as the data from quandle.  We are only going to need the first result of vxi2.
    zmvix =zip(months,(ql.get(a) for a in qc))


    #add an identifier column to  each dataframe
    #rather brute force
    def add_columns(df,tenor):
        #product identifier
        df["Tenor"] =  tenor
        df["Settlement Date"]=np.nan
        settlement_date = df['Settlement Date']
        for ix in  df.index:
            year,month,day = (ix.year,ix.month,ix.day)
            sd = vix_futures_settlement_date_from_trade_date(year, month, day, tenor)
            df.loc[ix,"Settlement Date"] = sd

            exchange_open_days = valid_days.loc[ix:sd]
            trade_days_to_settlement = len(exchange_open_days)
            #print(f"Trade {ix} Settle {sd} Trade days to settlement {trade_days_to_settlement}   ")
            df.loc[ix,"Trade Days to Settlement"]=trade_days_to_settlement

        df["Settlement Date"]=pd.to_datetime(df["Settlement Date"])
        df['Days to Settlement'] = ((df['Settlement Date'] - df.index).dt.days).astype(np.int16)

        return df

    #add in the settlment date and tenor columnss
    zmvix1= (  add_columns(df,m)           for m,df in zmvix)

    vix_all_months=  pd.concat(zmvix1)

    #print(f"\n {vix_all_months}")

    vix_all_months.to_parquet(cache_file)
else:
    vix_all_months=pd.read_parquet(cache_file)

#Now unstack
print(f"Vix all months {vix_all_months}")
print(f"\nVix All Months Close Columns {vix_all_months.columns}\n ")
cols = vix_all_months.columns
v2 = vix_all_months.reset_index()
print(f"v2 {v2}")
stacked = v2.pivot(columns="Tenor",index="Trade Date")
print(f"stacked {stacked}")
settlement_dates = vix_all_months['Settlement Date'].unique()
#print(f"Settlement Dates {settlement_dates}")
import matplotlib.pyplot as plt
import scipy.stats as bc

plt.plot(stacked['Close'])

plt.show()