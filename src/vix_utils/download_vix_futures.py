import aiohttp
import asyncio
import aiofiles
from appdirs import user_log_dir
from pathlib import Path
import itertools
from .vix_futures_term_structure import vix_futures_settlement_date_monthly
from .vix_futures_term_structure import vix_futures_settlement_date_from_trade_date

import datetime as dt
import numpy as np

import pandas_market_calendars as mcal
import calendar as cal
import pandas as pd
import more_itertools
import logging
from .location import data_dir,make_dir

#TODO
#https://www.cboe.com/us/futures/market_statistics/historical_data/archive/

def vix_settlements(start_year,end_year):
    j1_start=dt.date(start_year,1,1)
    dayofweek=j1_start.weekday()
    one_week=dt.timedelta(days=7)
    if dayofweek==2: #wednesday
        yield j1_start
    advance_days=(1-dayofweek) %7
    tuesday=j1_start+dt.timedelta(advance_days)               
    while tuesday.year < end_year:
        yield tuesday                  #the tuesday
        wednesday=tuesday + dt.timedelta(days=1)  #the wednesday
        if wednesday.year < end_year:
            yield wednesday
        tuesday=tuesday+one_week

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
        self.valid_days_set=frozenset(d.date() for d in self.valid_days.dt.to_pydatetime())

    #we might have some extra dates at the beginning and end 
    
cboe_futures_dates=CBOFuturesDates()



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

def start_year():
        return 2013
def stop_year():
    now = dt.datetime.now()
    return now.year+2

def years_and_months():
    now = dt.datetime.now()
    end_year=now.year+2

    return itertools.product( range(2011,end_year),range(1,13))

def archived_years_and_months():
    "For data from https://www.cboe.com/us/futures/market_statistics/historical_data/archive/"
    #specifically avoid 2013 since the data is dirty and duplicated with the
    #weekly and monthly data from the current download source.

    return list(itertools.product(range(2004,2012),range(1,13)))

def years_and_weeks():
    now = dt.datetime.now()
    end_year=now.year+2
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

def generate_weekly_url_date(date):
    """returns two possibilities"""
    date_str=date.isoformat()[0:10]
    url=generate_settlement_url(date_str)
 
    return date_str,url

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
            make_dir(p)
 
    async def download_one_monthly_future(self,year,month):
        url,expiry=generate_monthly_url_date(year,month)
        tag=f"m_{month}"
        save_path=self.futures_data_cache_monthly
        save_fn=f"{expiry}.m_{month}.CFE_VX_{year}.csv"        
        return await self.download_one_future(save_path,url,tag,expiry,fn=save_fn)
    
    async def download_one_archived_monthly_future(self, year, month):
        url,expiry=generate_archived_url_date(year,month)
        code=_futures_months_code[month-1]
        tag=f"m_{month}"
        save_path=self.futures_data_cache_archive_monthly
        save_fn=f"{expiry}.m_{month}.CFE_VX_{code}{year}.csv"
        return await self.download_one_future(save_path,url,tag,expiry,fn=save_fn)
 

    
    async def download_one_weekly_future(self,date):
        expiry,url=generate_weekly_url_date(date)

        tag=f"w_"  #we don't know the week #
        save_path=self.futures_data_cache_weekly
        year=date.year

        save_fn=f"{expiry}.w_.CFE_VX_{year}.csv"
        await self.download_one_future(save_path,url,tag,expiry,save_fn)
 
    
    async def download_one_future(self,save_path,url,tag,expiry,fn):
        ##Contract tag is a string to be stuck after the file name.  saved file will be
        #settlementdate.tag.{name from cboe}.
        #tag can be used to put in month or week.

        file_to_save=fn
        file_with_path=save_path/file_to_save
        pk_path=pk_path_from_csv_path(file_with_path)

        #if the pickle file corresponding to this csv has already been created, we don't need to download again.
        if(pk_path.exists()):
            return

        async with self.session.get(url) as response:
            if response.status !=  200:
                if dt.date.fromisoformat(expiry) < dt.date.today():
                    #this contract never traded, since we are trying every possible week.
                    #we don't want to download this next time through.
                    blank_csv="Trade Date,Futures,Open,High,Low,Close,Settle,Change,Total Volume,EFP,Open Interest"
                    
                    await dump_to_file(file_with_path,blank_csv)
                    #force an empty data frame to be saved as well
   
                    df=pd.DataFrame()
                    df.to_pickle(pk_path)

                return 
            headers=response.headers
            response_data=await response.read()
            await dump_to_file(file_with_path, response_data)
        
    async def download_monthly_futures(self):
        futures_to_download=(self.download_one_monthly_future(y,m) for (y,m) in years_and_months())
        await asyncio.gather(*futures_to_download)
    
    async def download_archived_monthly_futures(self):
        futures_to_download=(self.download_one_archived_monthly_future(y,m) for (y,m) in archived_years_and_months())
        await asyncio.gather(*futures_to_download)

    async def download_weekly_futures(self):
        dates=vix_settlements(start_year(),stop_year())
        futures_to_download=(self.download_one_weekly_future(d) for d in dates)
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
        #skip the monthly
        await asyncio.gather(v.download_weekly_futures(), v.download_monthly_futures(),v.download_archived_monthly_futures())

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

def pk_path_from_csv_path(csv_path):
    pkl_path=csv_path.with_suffix('.pkl')
    return pkl_path

def read_csv_future_files(vixutil_path):
        wfns,mfns,amfns=downloaded_file_paths(vixutil_path)
        cached_skinny_path=vixutil_path/"skinny.pkl"
        cached_skinny_expired_path=vixutil_path/"skinny_settled.pkl"
        logging.debug("reading cache")

        if cached_skinny_expired_path.exists():
            settled_frames=pd.read_pickle(cached_skinny_expired_path)
            already_expired=frozenset(settled_frames["File"])
        else:
            settled_frames=pd.DataFrame()
            already_expired=frozenset()
        logging.debug("read cache")

        monthly_settlement_date_strings=monthly_settlements(mfns)
        def read_csv_future_file(future_path):
            future_pkl_path=pk_path_from_csv_path(future_path)
            if future_pkl_path.exists():  #csv has already been turned into a dataframe, and has all the data for the settlement date.
                return pd.read_pickle(future_pkl_path)

            try:
                df = pd.read_csv(future_path,parse_dates=[0])
            except Exception as e:
                logging.warn(f"\n {e}\n reading\n{future_path} ")
                raise
            fn=future_path.name
            settlement_date_str=settlement_date_str_from_fn(fn)
            #week_number  TODO FIGURE THIS OUT
            monthly=settlement_date_str in monthly_settlement_date_strings
            df['Weekly']=not monthly
            #df['WeekOfYear']=?  TODO Figure this out
            df['Settlement Date']=settlement_date=pd.to_datetime(settlement_date_str).tz_localize('US/Eastern')
            df['Year']=settlement_date.year
            df['MonthOfYear']=settlement_date.month

            df['File']=fn

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
            #it is expensive to build this frame, largely due to localizing timestamps.
            #if it is complete, we save it.  we know it is complete (ie no more data points will be recorded in the future)
            #if the last timestamp is the settlment date


            last_row=df.iloc[-1]
            expired=last_row["Trade Date"]==last_row["Settlement Date"]
            df["Expired"]=expired
            if expired:
                df.to_pickle(future_pkl_path)
            return df
     
        #just read the weeklies, and the montlies prior to 2013.
        #the monthlys are the same as weeklies even though we did download the monthlies a second time we ignore them.
        #   
        def is_expired(fp):
            test_expired=fp.name in already_expired
            return test_expired
        logging.debug("Reading")
        #exclude reading the frames already in the cached data frame for futures expired.
        contract_history_frames=[read_csv_future_file(p) for  p in itertools.chain(wfns,amfns) if not is_expired(p)]
        logging.debug("Merging")

        futures_frame=pd.concat(itertools.chain([settled_frames],contract_history_frames),ignore_index=True)
        logging.debug("Column ordering")
        column_order=['Trade Date','Weekly','MonthTenor', 'Trade Days to Settlement','Days to Settlement', 'Settlement Date','Open', 'High',
       'Low', 'Close', 'Settle', 'Change', 'Total Volume', 'EFP',
       'Open Interest',   'Year', 'MonthOfYear','Futures',  'File','Expired' ]

        futures_frame.sort_values(by=["Trade Date","Settlement Date"])
        futures_frame_ordered_cols=futures_frame[column_order]
        futures_frame_expired=futures_frame_ordered_cols[futures_frame_ordered_cols["Expired"]==True]
        for df,p in ((futures_frame_ordered_cols,cached_skinny_path), (futures_frame_expired,cached_skinny_expired_path)): 
            df.to_pickle(p)

        return futures_frame_ordered_cols

def load_vix_term_structure():
    return asyncio.run(async_load_vix_term_structure())
    
async def async_load_vix_term_structure():
    global cfe_mcal, valid_days
    logging.debug("Getting Market calendar")
    cfe_mcal =  mcal.get_calendar('CFE')
    logging.debug("Got Market Calendar")
    #valid_days is expensive, so do it once here
    now=dt.datetime.now()
    #get info for futures expiring up to January 1 in six years.
    #no futures currently trade that far out so this should be fine
    five_years_away=dt.datetime(now.year+6,1,1)

    logging.debug("Valid days")
    valid_days=cfe_mcal.valid_days(start_date='2000-12-20', end_date=five_years_away).to_series();
    logging.debug("Got Valid days")

    user_path = data_dir()
    vixutil_path = user_path 
    make_dir(vixutil_path)
    do_download=True
    if do_download:
        logging.debug("Starting download futures")
        await download(vixutil_path)
        logging.debug("Downloaded  futures")
    rebuild=True
    if rebuild:
        df=read_csv_future_files(vixutil_path)
 
    return df

if __name__ == "__main__":  
    load_vix_term_structure()