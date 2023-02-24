import aiohttp
import asyncio
import aiofiles
from appdirs import user_data_dir,user_log_dir
from pathlib import Path
import itertools
import vix_futures_term_structure as t
import datetime as dt
import numpy as np

import pandas_market_calendars as mcal
import calendar as cal
import pandas as pd
import more_itertools


#TODO
#https://www.cboe.com/us/futures/market_statistics/historical_data/archive/

class CBOFuturesDates:
    def __init__(self):

        #move this to global, already duplicated in main
        self.cfe_mcal =  mcal.get_calendar('CFE')
        #valid_days is expensive, so do it once here
        now=dt.datetime.now()
        #get info for futures expiring up to January 1 in six years.
        #no futures currently trade that far out so this should be fine
        five_years_away=dt.datetime(now.year+6,1,1)

        self.valid_days=self.cfe_mcal.valid_days(start_date='2000-12-20', end_date=five_years_away).to_series();

    def vix_settlement_date_weekly(self, year,  week_number):
        c = cal.Calendar(cal.SUNDAY)

        m = c.monthdayscalendar(year, 1)
        md = c.monthdatescalendar(year, 1)
        # find the first wednesday of the year
        wednesday_index = 3
        wednesday_in_first_week = m[0][wednesday_index] != 0
        first_wednesday = md[0 if wednesday_in_first_week else 1][wednesday_index]
        settlement_date = first_wednesday + dt.timedelta(weeks=(week_number - 1))
        # no knowns special cases settlment dates for weekly settlement dates as of 2020-08-15

        return settlement_date
    
cboe_futures_dates=CBOFuturesDates()

vix_futures_settlement_date_monthly=t.vix_futures_settlement_date_monthly
vix_futures_settlement_date_from_trade_date=t.vix_futures_settlement_date_from_trade_date

#sample weeklky url: https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_2022-06-01.csv  for VX+VXT22/M2
#wich is also in the file name as CFE_VX_M2.csv

#sample monthly URL:https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_2023-01-18.csv  for VX+VXT/F3 (Jan 23)
#which is file name CFE_VX_F3.csv
_futures_months_code="FGHJKMNQUVXZ"
_futures_month_strings=[
"January",
"February",
"March",
"April",
"May",
"June",
"July",
"August",
"September",
"October",
"November",
"December"]
_cc="content-disposition"

_futures_month_numbers=list(range(1,len(_futures_month_strings)+1))

_futures_months_and_codes = list(zip(_futures_months_code,_futures_month_strings,_futures_month_numbers))

start_year=2013


def years_and_months():
    now = dt.datetime.now()
    end_year=now.year+2

    return itertools.product( range(2011,end_year),range(1,13))

def archived_years_and_months():
    "For data from https://www.cboe.com/us/futures/market_statistics/historical_data/archive/"
    #specifically avoid 2013 since the data is dirty and duplicated with the
    #weekly and monthly data from the current download source.
    print("Warning not downoading archived")

    return list(itertools.product(range(2004,2012),range(1,13)))[0:1]

def years_and_weeks():
    now = dt.datetime.now()
    end_year=now.year+2
    end_year=2014
    return itertools.product( range(2011,end_year),range(1,53))

def generate_settlement_url(date_str):
    return  f"https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_{date_str}.csv"

def generate_monthly_url_date(year,month):
    settlement_date=vix_futures_settlement_date_monthly(year,month)
    settlement_date_str=settlement_date.isoformat()[:10]
    url=generate_settlement_url(settlement_date_str)    
    return url,settlement_date_str
def generate_archived_url_date(year,month):
    code=_futures_months_code[month-1]
    settlement_date=vix_futures_settlement_date_monthly(year,month)
    settlement_date_str=settlement_date.isoformat()[:10]
    yy=settlement_date_str[2:4]
    url=f"https://cdn.cboe.com/resources/futures/archive/volume-and-price/CFE_{code}{yy}_VX.csv"
    return url, settlement_date_str

def generate_monthly_url_dates():
     return (generate_monthly_url_date(y,m) for y,m in years_and_months)

def generate_weekly_url_date(year,week):
    
    settlement_date=cboe_futures_dates.vix_settlement_date_weekly(year,week)
    settlement_date_str=settlement_date.isoformat()[:10]
    url=generate_settlement_url(settlement_date_str)    
    return url,settlement_date_str

async def dump_to_file(fn,data):
        try:
            async with aiofiles.open(fn,mode="wb") as f:
                await f.write(data)
        except Exception:
            async with aiofiles.open(fn,mode="w") as f:
                await f.write(data)

class VXFuturesDownloader:
    historical_data_url="https://www.cboe.com/us/futures/market_statistics/historical_data/"
    def __init__(self,data_dir,session):
        self.session=session
        #folder to store data files
        self.data_dir = data_dir
        self.futures_data=data_dir/"futures"
        self.futures_data_cache=data_dir/"futures"/"download"
        self.futures_data_cache_weekly=data_dir/"futures"/"download"/"weekly"
        self.futures_data_cache_monthly=data_dir/"futures"/"download"/"monthly"
        self.futures_data_cache_archive_monthly=data_dir/"futures"/"download"/"archive_monthly"
        for p in (self.futures_data_cache_weekly,self.futures_data_cache_monthly,self.futures_data_cache_archive_monthly):
            p.mkdir(exist_ok=True,parents=True) 
 
    async def download_one_monthly_future(self,year,month):
        url,expiry=generate_monthly_url_date(year,month)
        tag=f"m_{month}"
        save_path=self.futures_data_cache_monthly
        return await self.download_one_future(save_path,url,tag,expiry)
    
    async def download_one_archived_monthly_future(self, year, month):
        url,expiry=generate_archived_url_date(year,month)
        code=_futures_months_code[month-1]
        tag=f"m_{month}"
        save_path=self.futures_data_cache_archive_monthly
        save_fn=f"{expiry}.m_{month}.CFE_VX_{code}{year}.csv"
        return await self.download_one_future(save_path,url,tag,expiry,fn=save_fn)
 

    
    async def download_one_weekly_future(self,year,week):
        url,expiry=generate_weekly_url_date(year,week)
        tag=f"w_{week}"
        save_path=self.futures_data_cache_weekly
        return await self.download_one_future(save_path,url,tag,expiry)
    
    async def download_one_future(self,save_path,url,tag,expiry,fn=None):
        ##Contract tag is a string to be stuck after the file name.  saved file will be
        #settlementdate.tag.{name from cboe}.
        #tag can be used to put in month or week.
        async with self.session.get(url) as response:
            if response.status !=  200:
                return 
            headers=response.headers
            if fn is None:
                content_disposition=headers[_cc]  #content_disposition better have the file name
                cboe_filename=content_disposition.split('"')[-2] 
                file_to_save = f"{expiry}.{tag}.{cboe_filename}"
            else:
                file_to_save=fn 
            file_with_path=save_path/file_to_save
            response_data=await response.read()
            await dump_to_file(file_with_path, response_data)
        
    async def download_monthly_futures(self):
        futures_to_download=(self.download_one_monthly_future(y,m) for (y,m) in years_and_months())
        await asyncio.gather(*futures_to_download)
    
    async def download_archived_monthly_futures(self):
        futures_to_download=(self.download_one_archived_monthly_future(y,m) for (y,m) in archived_years_and_months())
        await asyncio.gather(*futures_to_download)

    async def download_weekly_futures(self):
        futures_to_download=(self.download_one_weekly_future(y,m) for (y,m) in years_and_weeks())
        await asyncio.gather(*futures_to_download)

    async def download_history_root(self):
        async with self.session.get(self.historical_data_url) as response:
            history_file=self.futures_data_cache/"history.html"
            history_headers_file=self.futures_data_cache/"history.txt"
            async with asyncio.TaskGroup() as tg:
                headers=f"{response.headers}"
                tg.create_task(dump_to_file(history_headers_file,headers))
                text=await response.read()
                tg.create_task(dump_to_file(history_file,text))

        return text     

def downloaded_file_paths(data_dir):
        """ returns a tuple (weekly,monthly,archive_monthly) list of Path objects
        """
        a=futures_data_cache_weekly=data_dir/"futures"/"download"/"weekly"
        b=futures_data_cache_monthly=data_dir/"futures"/"download"/"monthly"
        c=futures_data_cache_monthly=data_dir/"futures"/"download"/"archive_monthly"
        
        folders_contents=tuple( list(the_dir.glob("*.csv")) for the_dir in (a,b,c))

        return folders_contents  

async def download(vixutil_path):
    async with aiohttp.ClientSession() as session:
 
        v=VXFuturesDownloader(vixutil_path,session)
      
        await asyncio.gather(v.download_monthly_futures(),v.download_weekly_futures(), v.download_archived_monthly_futures())
#        await asyncio.gather(v.download_archived_monthly_futures())

        # #july-nov 2013 need to be fixed up by removing the first row.
        # cache_dir=vixutil_path/"futures"/"download"/"archive_monthly"
        # to_fix=[
        # "2013-07-17.m_7.CFE_VX_N2013.csv",
        # "2013-08-21.m_8.CFE_VX_Q2013.csv",
        # "2013-10-16.m_10.CFE_VX_V2013.csv",
        # "2013-11-20.m_11.CFE_VX_X2013.csv"]
        # for fn in to_fix:
        #     with open(fn,'r') as fin:
        #         data=fin.read().splitlines(True)
        #     with open(fn, 'w') as fout:
        #         fout.writelines(data[1:])

        #need to find lines with a trailing "," and remove it.  There are a bunch in the 
        #archived data
        _,_,amfns=downloaded_file_paths(vixutil_path)
        for fn in amfns:
            with open(fn,'r') as fin:
                data=fin.read().splitlines(True)

            with open(fn,'w') as fout:
                for line in data:
                    updated_line=",".join(line.split(",")[0:11]).strip()
                    print(updated_line,file=fout)



def settlement_date_str_from_fn(fn):
    return fn[:10]      # the leading iso date is 10 long

def monthly_settlements(monthly_paths):
    return frozenset(settlement_date_str_from_fn(p.name) for p in monthly_paths)

def week_number_from_fn(fn):
    return int(fn.split('.')[1].split("_")[1])


def read_csv_future_files(vixutil_path):
        wfns,mfns,amfns=downloaded_file_paths(vixutil_path)
        monthly_settlement_date_strings=monthly_settlements(mfns)
        def read_csv_future_file(future_path):
            try:
                df = pd.read_csv(future_path,parse_dates=[0])
            except Exception as e:
                print(f"\n {e}\n reading\n{future_path} ")
                raise
            fn=future_path.name
            settlement_date_str=settlement_date_str_from_fn(fn)
            week_number=week_number_from_fn(fn)
            monthly=settlement_date_str in monthly_settlement_date_strings
            df['Weekly']=not monthly
            df['WeekOfYear']=week_number
            df['Settlement Date']=settlement_date=pd.to_datetime(settlement_date_str).tz_localize('US/Eastern')
            df['Year']=settlement_date.year
            df['MonthOfYear']=settlement_date.month

            df['file']=fn           #just to help debugging

            df["Trade Date"]=df["Trade Date"].dt.tz_localize("US/Eastern")
            df['Days to Settlement']=((df['Settlement Date']-df['Trade Date']).dt.days).astype(np.int16)

            trade_dates = df['Trade Date']
            trade_days_to_settlement=pd.Series(index=df.index,dtype='int32')
            monthly_tenor=pd.Series(index=df.index,dtype='int32')
            settlement_date_local = settlement_date  
            look_ahead = 26 #look ahead 25 contracts to determine tenor

            unrealistic_future_tenor=[1001]
            for index, trade_date in trade_dates.items():
                
                trade_date_local=  trade_date  
                exchange_open_days = valid_days.loc[trade_date_local:settlement_date_local]

 
                trade_days_to_settlement.loc[index]=len(exchange_open_days)

                #find the next   settlement dates plus one way in the future
                #so that split_after always returns two items.


                next_settlements=list(vix_futures_settlement_date_from_trade_date(trade_date.year,trade_date.month,trade_date.day, tenor) \
                   for tenor in itertools.chain(range(1,look_ahead),unrealistic_future_tenor)) 
                 
                #figure out which monthly tenor applies here.  count the number of settlement dates less than
                # that contract settlment date.   
                #  
                settlement_date_py=settlement_date.date()
                def compare_settlement(s1):
                    return  settlement_date_py <= s1
#                try:
                (settlements_before_final,_)=more_itertools.split_after(next_settlements,compare_settlement,maxsplit=1)
 #               except Exception as e:
 #                   pass

                month_count=len(settlements_before_final)
                monthly_tenor.loc[index]=month_count

                #figure out which weekly tenor applies here.


            df.insert(0,"Trade Days to Settlement",trade_days_to_settlement)
            df.insert(0,"MonthTenor",monthly_tenor)
            return df
     
        #just read the weeklies, and the montlies prior to 2013.
        #the monthlys are the same as weeklies even though we did download the monthlies a second time we ignore them.
        #   
        contract_history_frames=[read_csv_future_file(p) for  p in itertools.chain(wfns,amfns)]
        futures_frame=pd.concat(contract_history_frames,ignore_index=True)
        futures_frame.set_index(["Trade Date"],inplace=True)
        column_order=['Weekly','MonthTenor', 'Trade Days to Settlement','Days to Settlement', 'Settlement Date','Open', 'High',
       'Low', 'Close', 'Settle', 'Change', 'Total Volume', 'EFP',
       'Open Interest',  'WeekOfYear', 'Year', 'MonthOfYear','Futures',  'file' ]

        futures_frame.sort_index(inplace=True)
        futures_frame_ordered_cols=futures_frame[column_order]
        return futures_frame_ordered_cols


async def main():
    global cfe_mcal, valid_days
    cfe_mcal =  mcal.get_calendar('CFE')
    #valid_days is expensive, so do it once here
    now=dt.datetime.now()
    #get info for futures expiring up to January 1 in six years.
    #no futures currently trade that far out so this should be fine
    five_years_away=dt.datetime(now.year+6,1,1)

    valid_days=cfe_mcal.valid_days(start_date='2000-12-20', end_date=five_years_away).to_series();


    user_path = Path(user_data_dir())
    vixutil_path = user_path / ".vixutil"
    vixutil_path.mkdir(exist_ok=True)
    do_download=True
    if do_download:
        await download(vixutil_path)
    rebuild=True
    if rebuild:
        df=read_csv_future_files(vixutil_path)
        df.to_pickle(vixutil_path/"skinny.pkl")
    df=pd.read_pickle(vixutil_path/"skinny.pkl")

    
asyncio.run(main())    