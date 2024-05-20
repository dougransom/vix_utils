import aiohttp
import asyncio
import aiofiles
from appdirs import user_log_dir
from pathlib import Path
import itertools
from .vix_futures_dates import vix_futures_expiry_date_monthly
from .vix_futures_dates import vix_futures_expiry_date_from_trade_date
from .futures_utils import timeit
import datetime as dt
import numpy as np

import pandas_market_calendars as mcal
import calendar as cal
import pandas as pd
import more_itertools
import logging
from .location import data_dir,make_dir
from collections.abc import Generator
_cached_vix_futures_records = None
_date_cols=["Trade Date","Expiry"]
_duplicate_check_subset=['Trade Date','Expiry']

_column_order=['Trade Date','Weekly','Tenor_Monthly', 'Tenor_Days','Tenor_Trade_Days', 'Expiry','Open', 'High',
       'Low', 'Close', 'Settle', 'Change', 'Total Volume', 'EFP',
       'Open Interest',   'Year', 'MonthOfYear','Futures',  'File','Expired' ]

_value_columns=['Open', 'High', 'Low', 'Close', 'Settle', 'Change']

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

def vix_settlements(start_year:int,end_year:int) ->Generator[dt.date,None,None]:
    """make an iterator that yields the possible settlments for every week.  we assume every tuesday and 
    wedensday can be a settlement.  """
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

def start_year()->int:
        return 2013
def stop_year()->int:
    now = dt.datetime.now()
    return now.year+2

def years_and_months():
    now = dt.datetime.now()
    end_year=now.year+2

    return itertools.product( range(2011,end_year),range(1,13))

def archived_years_and_months():
    "For data from https://www.cboe.com/us/futures/market_statistics/historical_data/archive/"
 

    return list(itertools.product(range(2004,2014),range(1,13)))

def years_and_weeks():
    now = dt.datetime.now()
    end_year=now.year+2
    return itertools.product( range(2011,end_year),range(1,53))

def generate_settlement_url(date_str:str) -> str:
    return  f"https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_{date_str}.csv"

def generate_monthly_url_date(year:int,month:int) -> tuple[str,str]:
    """ Returns the URL and a date string for the expiry date used to build the url.
    parameters:
    -----------
    year: year of expiry
    month: month of expiry

    """
    settlement_date=vix_futures_expiry_date_monthly(year,month)
    settlement_date_str=settlement_date.isoformat()[:10]
    url=generate_settlement_url(settlement_date_str)    
    return url,settlement_date_str

def generate_archived_url_date(year,month) -> tuple[str,str]:
    """ Returns the URL and a date string for the expiry date used to build the url for 
    `CBOE Archive data<https://www.cboe.com/us/futures/market_statistics/historical_data/archive/>`_ from 2004-2013.  
    parameters:
    -----------
    year: year of expiry
    month: month of expiry

    """

    code=_futures_months_code[month-1]
    settlement_date=vix_futures_expiry_date_monthly(year,month)
    settlement_date_str=settlement_date.isoformat()[:10]
    yy=settlement_date_str[2:4]
    url=f"https://cdn.cboe.com/resources/futures/archive/volume-and-price/CFE_{code}{yy}_VX.csv"
    return url, settlement_date_str

def generate_monthly_url_dates():
     """
     Returns  the possible monthly urls and the date strings for all years and months in the default range.
     """
     return (generate_monthly_url_date(y,m) for y,m in years_and_months())

def generate_weekly_url_date(date):
    """
    Returns the URL for the pattern of CBO to retreive the vix future expiring on date, even
    if no futures expired on date (which would give an URL to a non-existant resource)
    """
    date_str=date.isoformat()[0:10]
    url=generate_settlement_url(date_str)
 
    return date_str,url

async def dump_to_file(fn,data):
        """
        Save data to a file.
        parameters:
        ===========
        fn:  file name, which can be a string or path like object.
        data:  the data to write to the file.  can be binary or text.
        """
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
 
    async def download_one_monthly_future(self,year:int,month:int):
        """
        Download a monthly future from CBOE, 2013+.
        parameters:
        year:
        month:        
        """
        url,expiry=generate_monthly_url_date(year,month)
        save_path=self.futures_data_cache_monthly
        save_fn=f"{expiry}.m_{month}.CFE_VX_{year}.csv"        
        return await self.download_one_future(save_path,url,expiry,fn=save_fn)
    
    async def download_one_archived_monthly_future(self, year:int, month:int):
        """
        Download a future from the CBOE 'archive' data (2004-2013)
        parameters:
        year:
        month:        
        """
        url,expiry=generate_archived_url_date(year,month)
        code=_futures_months_code[month-1]
        save_path=self.futures_data_cache_archive_monthly
        save_fn=f"{expiry}.m_{month}.CFE_VX_{code}{year}.csv"
        return await self.download_one_future(save_path,url,expiry,fn=save_fn)
 

    
    async def download_one_weekly_future(self,date:dt.datetime):
        """
        Download a weekly future from CBOE for the date.  Save a dummy CSV file with no
        records if it doesn't exist.
        parameters:
        -----------
        date: date of futures expiry
        """
        expiry,url=generate_weekly_url_date(date)

        save_path=self.futures_data_cache_weekly
        year=date.year

        save_fn=f"{expiry}.w_.CFE_VX_{year}.csv"
        await self.download_one_future(save_path,url,expiry,save_fn)
 
    
    async def download_one_future(self,save_path:Path,url:str,expiry:str,fn:str):
        """
        Download one future and save it to path specfied.  
        parameters:
        ----------_
        save_path:  path to where the file will be saved.
        expiry: expiry of the future in string format
        fn:  file name to be tacked on to save_path before writing the CSV
        """
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

def downloaded_file_paths(data_dir:Path)->tuple[Path,Path,Path]:
        """ returns a tuple (weekly,monthly,archive_monthly) list of Path objects
            where weekly,monthly, and archive_monthly (2013 and earlier) downloads are stored.
        """
        a=futures_data_cache_weekly=data_dir/"futures"/"download"/"weekly"
        b=futures_data_cache_monthly=data_dir/"futures"/"download"/"monthly"
        c=futures_data_cache_monthly=data_dir/"futures"/"download"/"archive_monthly"


        folders_contents=tuple( list(the_dir.glob("*.csv")) for the_dir in (a,b,c))

        return folders_contents  
_header_match_str="Trade Date,"
_head_match_len=len(_header_match_str)
async def download(vixutil_path:Path):
    """
    Download the vix futures historis we don't have up todate.
    Fixup broken files.
    parameters:
    -----------
    vixutil_path.   Root of where information is cached, like previously downloaded or generated files.

    """
    async with aiohttp.ClientSession() as session:
 
        v=VXFuturesDownloader(vixutil_path,session)
        #skip the monthly
        await asyncio.gather(v.download_weekly_futures(), v.download_monthly_futures(),v.download_archived_monthly_futures())


        #need to find lines with a trailing "," and remove it.  There are a bunch in the 
        #archived data.
        #we also need to throw away lines before the line that starts with Trade Date.
        _,_,amfns=downloaded_file_paths(vixutil_path)
        
        def unmatched_header_row(a_line):
            return a_line[0:_head_match_len]!=_header_match_str

        for fn in amfns:
            with open(fn,'r') as fin:
                data=fin.read().splitlines(True)

            filtered_preamble=itertools.dropwhile(unmatched_header_row,data)
            with open(fn,'w') as fout:
                for line in filtered_preamble:
                    updated_line=",".join(line.split(",")[0:11]).strip()
                    print(updated_line,file=fout)



def settlement_date_str_from_fn(fn:str)->str:
    """
    get the Expiry from the file name.
    parameters:
    -----------
    fn: file name (without the path)   

    """
    return fn[:10]      # the leading iso date is 10 long

def monthly_settlements(monthly_paths)->frozenset[str]:
    """
    return all the Expirys (as strings) that are in the downloaded monthly futures. 
    """
    return frozenset(settlement_date_str_from_fn(p.name) for p in monthly_paths)

def week_number_from_fn(filename:str)->int:
    """
    get the week number from the file name.
    filename:  the name of a file downloaded and saved.
    """
    return int(filename.split('.')[1].split("_")[1])

def pk_path_from_csv_path(csv_path:Path)->Path:
    """
    returns a path for a pickle (.pkl) file corresponding to a csv file.
    parameters:
    -----------
    csv_path.  Path to a csv file
    """
    pkl_path=csv_path.with_suffix('.pkl')
    
    return pkl_path

def read_csv_future_file(future_path:Path,monthly_expiry_date_strings:frozenset)->pd.DataFrame:
    """
    Read the csv file in to a data frame, add in the monthly tenor and some calulated columns.
    parameters:
    -----------
    future_path:   the path to the file to load.
    monthly_expiry_date_strings:  strings that contain all the monthly Expirys
    """
    future_pkl_path=pk_path_from_csv_path(future_path)
    if future_pkl_path.exists():  #csv has already been turned into a dataframe, and has all the data for the Expiry.
        return pd.read_pickle(future_pkl_path)

    try:
        df = pd.read_csv(future_path,parse_dates=[0])
    except Exception as e:
        logging.warn(f"\n{e}\n reading\n{future_path}, skipping ")
        return pd.DataFrame(columns=[["Trade Date"]])

    fn=future_path.name
    expiry_date_str=settlement_date_str_from_fn(fn)
    #week_number  TODO FIGURE THIS OUT
    monthly=expiry_date_str in monthly_expiry_date_strings
    df['Weekly']=not monthly
    #df['WeekOfYear']=?  TODO Figure this out
    df['Expiry']=expiry_date=pd.to_datetime(expiry_date_str).tz_localize('US/Eastern')
    df['Year']=expiry_date.year
    df['MonthOfYear']=expiry_date.month

    df['File']=fn

    df["Trade Date"]=df["Trade Date"].dt.tz_localize("US/Eastern")
    df['Tenor_Days']=((df['Expiry']-df['Trade Date']).dt.days).astype(np.int16)

    #remove any errenous rows with an entry later than the expiry day.
    #there are a few of these  in 2004-2005.
    df=df[df["Tenor_Days"]>=0]

    trade_dates = df['Trade Date']
    trade_days_to_expiry=pd.Series(index=df.index,dtype='int32')
    monthly_tenor=pd.Series(index=df.index,dtype='int32')
    expiry_date_local = expiry_date  
    look_ahead = 40 #look ahead 25 contracts to determine tenor

    unrealistic_future_tenor=[1001]
    for index, trade_date in trade_dates.items():
        
        trade_date_local=  trade_date  
        exchange_open_days = valid_days.loc[trade_date_local:expiry_date_local]


        trade_days_to_expiry.loc[index]=len(exchange_open_days)

        #find the next   Expirys plus one way in the future
        #so that split_after always returns two items.


        next_settlements=list(vix_futures_expiry_date_from_trade_date(trade_date.year,trade_date.month,trade_date.day, tenor) \
            for tenor in itertools.chain(range(1,look_ahead),unrealistic_future_tenor)) 
            
        #figure out which monthly tenor applies here.  count the number of Expirys less than
        # that contract settlment date.   
        #  
        settlement_date_py=expiry_date.date()
        def compare_settlement(s1):
            compare =  settlement_date_py <= s1  #leave this temp in for debugging
            return compare

        (settlements_before_final,_)=more_itertools.split_after(next_settlements,compare_settlement,maxsplit=1)


        month_count=len(settlements_before_final)
        monthly_tenor.loc[index]=month_count

        #figure out which weekly tenor applies here.


    df.insert(0,"Tenor_Trade_Days",trade_days_to_expiry)
    df.insert(0,"Tenor_Monthly",monthly_tenor)
    #it is expensive to build this frame, largely due to localizing timestamps.
    #if it is complete, we save it.  we know it is complete (ie no more data points will be recorded in the future)
    #if the last timestamp is the settlment date


    last_row=df.iloc[-1]
    expired=last_row["Trade Date"]==last_row["Expiry"]
    df["Expired"]=expired
    if expired:
        df.to_pickle(future_pkl_path)
    return df
     
   
def read_csv_future_files(vixutil_path:Path)->pd.DataFrame:
        """
        read the downloaded files into data frames.
        read the cached data frames from previous runs of this function.
        return a DataFrame of all vix futures history records, in a skinny (record) format.
        """
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
        #we use the downloaded file names as a list of the Expirys.
        #it might be smarter to use the vix settlments dates instead

        monthly_expiry_date_strings=monthly_settlements(itertools.chain(mfns,amfns))
        #just read the weeklies, and the montlies prior to 2013.
        #the monthlys are the same as weeklies even though we did download the monthlies a second time we ignore them.
        #   
        def is_expired(fp):
            test_expired=fp.name in already_expired
            return test_expired
        logging.debug("Reading")
        #exclude reading the frames already in the cached data frame for futures expired.
        contract_history_frames=[read_csv_future_file(p,monthly_expiry_date_strings) for  p in itertools.chain(wfns,amfns) if not is_expired(p)]
        logging.debug("Merging")

        futures_frame=pd.concat(itertools.chain([settled_frames],contract_history_frames),ignore_index=True)
        logging.debug("Column ordering")

        futures_frame_ordered_cols=futures_frame.sort_values(by=_date_cols)[_column_order]
        futures_frame_expired=futures_frame_ordered_cols[futures_frame_ordered_cols["Expired"]==True]

        #a few days where there is weird rows of 0s except perhaps the settlement column
        #remove them.
        futures_frame_ordered_cols=futures_frame_ordered_cols[futures_frame_ordered_cols["Close"]!=0]

        #we have seen duplicates in the downloaded data.
        #for example, FEb 27 and Feb 28 trade dates, duplicated for the 2020-03-25 settlement.


        duplicated=futures_frame_ordered_cols[futures_frame_ordered_cols.duplicated(subset=_duplicate_check_subset,keep=False)]


        if duplicated.shape[0] > 0:
            logging.warning(f"\nDuplicates detected\n{duplicated}, cleaning them out")
            futures_frame_ordered_cols.drop_duplicates(inplace=True,subset=_duplicate_check_subset)

        for df,p in ((futures_frame_ordered_cols,cached_skinny_path), (futures_frame_expired,cached_skinny_expired_path)): 
            df.to_pickle(p)

        return futures_frame_ordered_cols

 


def load_vix_term_structure(forceReload=False):
    """
    returns the vix futures history in skinny (record) format.  This will return a copy
    of the last dataframe returned by load_vix_term_structure retained in memory unless forceReload
    is specified.
    Note that this is meant to be called from programs which don't have an event loop.  Use async_load_vix_term_structure
    if you have an event loop.

    parameters:
    -----------
    forceReload: specify that the DataFrame should be rebuilt, rather than using the DataFrame cached in memory from the 
    last call, if one exists.

    """
    return asyncio.run(async_load_vix_term_structure(forceReload))
#    with asyncio.Runner() as runner:
#        return runner.run(async_load_vix_term_structure(forceReload))

@timeit()    
async def async_load_vix_term_structure(forceReload=False)->pd.DataFrame:
    """
    returns the vix futures history in skinny (record) format.  This will return a copy
    of the last dataframe returned by async_load_vix_term_structure retained in memory unless forceReload
    is specified.
    Note that this is meant to be called from programs which  have an event loop.  Use load_vix_term_structure
    if you have don't have an event loop.

    parameters:
    -----------
    forceReload: specify that the DataFrame should be rebuilt, rather than using the DataFrame cached in memory from the 
    last call, if one exists.

    """

    async def reload_vix_futures_history()->pd.DataFrame:
        """
            Reload the vix futures history, downloading any necessary files.  Avoid downloading files
            already  in the cache we know are current.
        """
 

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
        #remove the timezone info

        return df

    global _cached_vix_futures_records

    if forceReload or _cached_vix_futures_records is None:
        _cached_vix_futures_records=await reload_vix_futures_history()
 
    futures_frame=_cached_vix_futures_records.copy(deep=True)

    #scale data prior to march 26, 2007.  see data_notes.md

    prior_to_scale_change=futures_frame["Trade Date"] <= '2007-03-26'
    scale_df = futures_frame[_value_columns].copy(deep=True)*0.1
    #replace the scaled values in the correct date range
    scaled_futures_frame=futures_frame.mask(prior_to_scale_change,other=scale_df)
    futures_frame[_value_columns]=scaled_futures_frame[_value_columns]
    
    #remove localizatoin

    for d in _date_cols:
        futures_frame[d]=futures_frame[d].dt.tz_localize(None) 

    return futures_frame
    
def select_monthly_futures(vix_futures_records:pd.DataFrame)->pd.DataFrame:
    """Return only the records with monthly expirys, filtered from vix_futures_records.  
    if no 'Weekly' column exists then return all the records of vix_futures_records.  This might
    happen if select_monthly_futures is called twice on the same set of records. 
    parameters:
    -----------
    vix futures_records:  the futures history in records format, as downloaded by load_vix_term_structure.
        

    """    
    monthly = vix_futures_records[vix_futures_records['Weekly'] == False].drop('Weekly',axis=1) \
    if "Weekly" in vix_futures_records.columns else pd.DataFrame(vix_futures_records)

    return monthly

_stars="*"*30

def pivot_futures_on_monthly_tenor(vix_futures_records:pd.DataFrame)->pd.DataFrame:
    """
    First filters the future history in record format to only have monthly records.  Typically the data
    frame downloaded by load_vix_term_structure.  

    Sets the index to (Trade Date, Tenor_Monthly) and does an unstack, so the values of Tenor_Monthly become the first level
    column index.

    """
    #if anything goes wrong in the data (ie. it isn't clean from CBOE), it is likely to cause a problem 
    # in the unstack operation.  
    #it is hard to debug so leave the exception handling and 
    # the temp varables so breakpoints can be set if necessary.
    dups_str=""
    with pd.option_context('display.max_columns',None):
        try:    
            monthly=select_monthly_futures(vix_futures_records)
            monthly_indexed=monthly.set_index(["Trade Date","Tenor_Monthly"])
            dups=monthly_indexed[monthly_indexed.index.duplicated(keep=False)]
            #easier to debug with a select set of columns to display.
            debug_cols=["File","Tenor_Trade_Days", "Expiry","Close"]
            with pd.option_context('display.max_rows',None):
                if dups.shape[0]>0:         #a common cause of problems are duplicate trade/tenors.
                                            #CBOE may publish overlapping files with same dates.
                    logging.warn(f"\n{_stars}Duplicates detected for Trade Date and Tenor\n")
            
                dups_str=f"{dups[debug_cols]}"
            #remove the duplicates https://stackoverflow.com/questions/13035764/remove-pandas-rows-with-duplicate-indices
            monthly_indexed=monthly_indexed[~monthly_indexed.index.duplicated(keep='first')]

            unstacked = monthly_indexed.unstack()
            pivoted=unstacked.swaplevel(0,1,axis=1)
        except Exception as e:
            msg=(f"\n{_stars}Caught {e} in module in function {__name__}" 
            f"\n{_stars}\nduplicates in the monthly futures:\n{dups_str}")
            logging.error(msg)
            raise RuntimeError(msg) from e
 

    return pivoted
