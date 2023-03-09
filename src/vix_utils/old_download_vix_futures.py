import asyncio
from appdirs import user_data_dir,user_log_dir
import logging
import re
import sys
from typing import IO
import urllib.error
import urllib.parse
from ./vix_futures_term_structure  import vix_futures_settlement_date_monthly
import aiofiles
import aiohttp
from aiohttp import ClientSession,TCPConnector
from itertools import chain
from more_itertools import flatten
from bs4 import BeautifulSoup
import csv
import pandas as pd
from io import StringIO,BytesIO
import time
import calendar as cal
import datetime as dt
import numpy as np
import os
from pathlib import Path
import pandas_market_calendars as mcal
import socket
import concurrent.futures
#set to true to limit download to a sample of files from CBOE
limit_download_files_for_development=False
max_download_files_for_development = 10

#Futures Months Codes
#http://www.cboe.com/products/futures/trade-cfe/quote-vendor-symbols/vx-cboe-volatility-index-vix-futures

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

_futures_month_numbers=list(range(1,len(_futures_month_strings)+1))

_futures_months_and_codes = list(zip(_futures_months_code,_futures_month_strings,_futures_month_numbers))


#constants for parsing the URLs
#strings look like 'VX+VXT52 Z (Dec 15) for weeklys or
# 'VX+VXT Z (Dec 20)' for monthlys

_monthly_first_token = "VX+VXT"
_length_monthly_token = len(_monthly_first_token)
_weekly_symbol_suffix_len=2 #for 2 numbers that follow the symbol
_length_weekly_token = _length_monthly_token+_weekly_symbol_suffix_len

#some constants for parse_vix_futures_file_name        
_len_monthly = len("CFE_Z19_VX.csv")
_len_weekly = len("CFE_Z19_VX01.csv")

#sample weeklky url: https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_2022-06-01.csv  for VX+VXT22/M2
#wich is also in the file name as CFE_VX_M2.csv

#sample monthly URL:https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_2023-01-18.csv  for VX+VXT/F3 (Jan 23)
#which is file name CFE_VX_F3.csv


#we can't download a list of URLs anywhere, at least not easily.


#the slice for the week number is from 10 to 12 in the file name
#just hard code it.
_index_of_month_code=4   
_products_url = "https://markets.cboe.com/us/futures/market_statistics/historical_data/products/"
_vx_url = _products_url+"VX"

_data_root_url="https://www.cboe.com/us/futures/market_statistics/historical_data/"
 

_xm = 'XMonth'
_ym = 'YMonth'
_sd = "Expiry"
_td ="Trade Date"

#the columns that will be in the dataframe

_final_cols=[ _nth := 'Nth Month', 'Close', 'Open', 'High', 'Low', 'Settle', 'Change',
       'Total Volume', 'EFP', 'Open Interest', 
       _dts:='Tenor_Trade_Days', _tdts:= 'Tenor_Days',_mdts:="Measured Tenor_Days",
       _cdts:="Calculated Tenor_Days",
       'Futures',"File Name"]

#the columns which will be categorical
_categorical = ['Month Code', 'Month Name',
        'Symbol Root','Futures','Frequency','File Name']
_str_columns = ['Month Name','Symbol Root','Month Code','Futures','File Name']

#for dealing with the HTTP response
_cc="content-disposition"

#date handling for series still open
#http://www.cboe.com/products/futures/vx-cboe-volatility-index-vix-futures/contract-specifications
#Final Expiry:
#The final Expiry for a contract with the "VX" ticker symbol is on the Wednesday that is 30 days
#  prior to the third Friday of the calendar month immediately following
#  the month in which the contract expires. 
# The final Expiry for a futures contract with 
# the "VX" ticker symbol followed by a number denoting the specific week of a calendar 
# year is on the Wednesday of the week specifically denoted in the ticker 
# symbol.
#
#If that Wednesday or the Friday that is 30 days following that Wednesday is a Cboe Options holiday,
#the final Expiry for the contract shall be on the business day immediately preceding that Wednesday.
#VX Futures Symbols - VX* and VX01 through VX53**. Embedded numbers denote the 
# specific week of a calendar year during which a contract is settled. For
#  symbology purposes, the first week of a calendar year is the first
#  week of that year 
# with a Wednesday on which a weekly VX futures contract could expire.


class VXFuturesDownloader:
    def __init__(self,data_dir):

        #folder to store data files
        self.data_dir = data_dir
        self.futures_data=data_dir/"futures"
        self.futures_data_cache=data_dir/"futures"/"download"

        #path to store futures dataframe
        self.parquet_path = self.futures_data_dir + "/vix_futures.parquet"
        #path to store continous futures dataframe
        self.cf_parquet_path = self.futures_data_dir + "/vix_continous_futures.parquet"
        #calendar used to determine trading Tenor_Trade_Days
        self.cfe =  mcal.get_calendar('CFE')
        #valid_days is expensive, so do it once here
        now=dt.datetime.now()
        #get info for futures expiring up to January 1 in six years.
        #no futures currently trade that far out so this should be fine
        five_years_away=dt.datetime(now.year+6,1,1)

        self.valid_days=self.cfe.valid_days(start_date='2000-12-20', end_date=five_years_away).to_series();
        self.futures_df=None
        self.executor=None
    def _executor(self):
        if self.executor==None:
            self.executor=executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)

    async def get_data(self):
        if self.futures_df is not None:
            df=self.futures_df.copy()
            trade_date=df["Trade Date"]
            latest_trade=trade_date.max()
            return df
        try: 
            pq_time=dt.datetime.fromtimestamp(os.path.getmtime(self.parquet_path))
            pd_now = pd.Timestamp.now(tz='US/Eastern')
            dt_now = dt.datetime.now()
 
            #need something better, but for now limit updates to 30 minutes
            if dt_now - pq_time < dt.timedelta(minutes=30):
                #use the file cache
                async with aiofiles.open(self.parquet_path, mode='rb') as f:
                    raw_data = await f.read()
                    contents = BytesIO(raw_data)
                df = pd.read_parquet(contents)
                self.futures_df=df
                return await self.get_data()
        except:
            pass
            #if we get here, it means no cached dataframe
            #do all the work to load the VIX data
       #family = socket.AF_INET to limit to ipv4.
        connector=TCPConnector(force_close=False, limit_per_host=30)
        t1=time.time()
        async with ClientSession(connector=connector) as session:
            cr=await self.get_contract_file_names_and_urls(_vx_url,session)
            df = await self.download_the_files(session,cr)
        t2=time.time()
        loop = asyncio.get_running_loop()
        async def frame_to_parquet(filename,dataframe):          
            parquet_raw = BytesIO()
            await loop.run_in_executor(self._executor(),dataframe.to_parquet,parquet_raw)
            data_to_write=parquet_raw.getvalue()
            async with aiofiles.open(filename, mode='wb') as f:
                await f.write(data_to_write)
        continous_df_coro=loop.run_in_executor(self._executor(),self._build_continous_monthly_vix_futures,df)
        df_coro = frame_to_parquet(self.parquet_path,df)
        (continous_df,_)=await asyncio.gather(continous_df_coro,df_coro)
        self.continous_df = continous_df
        self.futures_df = df
        await frame_to_parquet(self.cf_parquet_path,self.continous_df)

        return df

    async def get_a_vix_index_history(self,session,url,num_lines_to_discard,symbol,fixup_columns):
        resp = await session.get(url)
        history = await resp.text()
        reader = StringIO(history)
        for ix in range(num_lines_to_discard):
            discard = reader.readline()
        df = pd.read_csv(reader, parse_dates=[0])
        df['Symbol Root'] = symbol
        df = fixup_columns(df)
        print(f"{symbol}  history \n{df}")
        return df



    def get_data_sync(self):
        loop = asyncio.get_event_loop()
        df=loop.run_until_complete(self.get_data())
        return df

    def settlement_date_weekly(self,year,month,week_number):
        c=cal.Calendar(cal.SUNDAY)

        m=c.monthdayscalendar(year,1)
        md=c.monthdatescalendar(year,1)
        #find the first wednesday of the year
        wednesday_index=3
        wednesday_in_first_week = m[0][wednesday_index] != 0
        first_wednesday = md[0 if wednesday_in_first_week else 1][wednesday_index]
        settlement_date=first_wednesday+dt.timedelta(weeks = (week_number-1) )
        #        print(f"Weekly {year} {month} {week_number}  Expiry: {settlement_date}")
        #no knowns special cases settlment dates for weekly Expirys as of 2020-08-15

        return settlement_date

    def settlement_date_monthly(self,year,month):
        c=cal.Calendar(firstweekday=cal.SUNDAY)
        next_month=month+1

        #does the option series the future settles on  settle next year?
        options_next_year = next_month > 12
        
        next_month = next_month % 12 if options_next_year else next_month 
        options_year = year+1 if options_next_year else  year 
        
        m=c.monthdayscalendar(year,next_month)
        md=c.monthdatescalendar(year,next_month)

        friday_index=-2
        #2 to index the 3d week, 0 based index for m
        week_index = 2 if m[0][friday_index] != 0 else 3
        #third_friday unused, just for easier debugging to have.
        third_friday = m[week_index][friday_index]
        option_expiry_date = md[week_index][friday_index]
        futures_expiry_date = option_expiry_date-dt.timedelta(days=30)
        #no knowns special cases for monthly vix futures settlment dates as of 2020-08-15
#        print(f"Year {year} month {month} options {option_expiry_date} futures {futures_expiry_date}  ")
        return futures_expiry_date

  

    def description_to_details(self,str):
        tokens = str.split()
        first_token = tokens[0]
        second_token=tokens[1]
        first_token_len = len(first_token)
        monthly = _monthly_first_token == first_token
        if not monthly:
            weekly = first_token[:_length_monthly_token] == _monthly_first_token and \
                first_token_len == _length_weekly_token and (all(x.isdigit() for x in first_token[-_weekly_symbol_suffix_len:]) )

        if not (monthly or weekly):
            raise ValueError(f"Bad Symbol {first_token}" )

        month_code = second_token
        month_short_name = tokens[2][1:]  #delete the leading parenthesis
        short_year = tokens[3][:-1]  #delete the trailing parenthisis
        #Y2K1 alert  - year is only 2 digits in the CBOE file as of may 2020
        year = "20"+short_year
        return (first_token, monthly, month_code,month_short_name,year)


    async def get_contract_names_from_vix_full_history(self,url,session):
        async with session.get(url) as resp:
            text=await(resp.text())
            soup = BeautifulSoup(text,"html.parser")
        #for now ignore the results from the url
        #instead just generate the URLs
        #we know they look like this:
        _vx_start_year=2005
        next_year =  1+dt.datetime.now().year



    async def get_contract_file_names_and_urls(self,url,session):

        async with session.get(url) as resp:
            text=await(resp.text())
            soup = BeautifulSoup(text,"html.parser")
            anchors = soup.select('.historical-data-link')

            contract_spec_and_url = (  (a.string.strip(), a["href"].strip()) for a in anchors )
            strs,links = zip(*contract_spec_and_url)
            symbol_details =  ( self.description_to_details(y) for y in strs)
            symbol_details_u = zip(*symbol_details)

            all_columns_as_rows = chain(symbol_details_u, [links])
            contracts_and_locations = list(zip(*all_columns_as_rows))
            return contracts_and_locations
    


    async def download_a_file(self,session,url,file):
        #the urls are alwys ending in an /isodate/
        #so splitting on / and indexing by -2 will get the date string
        settlement_date_str = url.split('/')[-2]
        #see if we have the data already, and if it is current

        files_matched=list(Path(self.data_dir).glob(f"{settlement_date_str}.*.csv"))
        if len(files_matched) == 1:
            matched_file=files_matched[0]
            matched_file_name=matched_file.name
            pq_time=dt.datetime.fromtimestamp(os.path.getmtime(matched_file))
            pd_now = pd.Timestamp.now(tz='US/Eastern')
            dt_now = dt.datetime.now()
            settlement_date = dt.date.fromisoformat(settlement_date_str)
            #if the Expiry is before the file date
            #we don't need to download the file again
            #load it from disk instead
            if settlement_date < pq_time.date():
                data_to_read =  await self.read_futures_file(matched_file_name)
                loop=asyncio.get_running_loop()
                df = await  loop.run_in_executor(self._executor(),self.process_futures_file,data_to_read,matched_file_name)
                return (matched_file_name,df)

            #A possible optimization is to avoid downloading 
            #the currently trading futures if we already have the most 
            #current file.  Currently, all trading contracts will be downloaded.  
            #not a huge deal as there are usually around 12


        await asyncio.sleep(0)
        try:
            async with session.get(url) as resp:

                if resp.status !=  200:
                        raise ValueError(f"HTTP Error {url}" )
                headers=resp.headers

                #just let it blow up if there is no content-disposition
                content_disposition=headers[_cc]
                #print(f"\nContent Disposition {content_disposition}")
                #print(f"Downloaded {url}  headers f{headers} file {content_disposition}")

                #content disposiutin used to look like   'attachment;filename="CFE_F21_VX.csv"'
                #now it looks like 'Content Disposition attachment;filename="CFE_VX/H3.csv"'  and it will may change at some point when contracts expiring in
                #20203 exist.
                # for weekly it will look like Content Disposition attachment;filename="CFE_VX35/Q5.csv"


                #prepend the settlment date from the URL to the file specified by
                #content disposition
                cboe_filename= content_disposition.split('"')[-2]  #will look like CFE_VK/H3.csv or for weekly, CFE_VX35/Q5.csv
                #we are going to replace the "/" with a "."
                cfile_to_save = cboe_filename.replace('/','.')

                file_to_save = f"{settlement_date_str}.{cfile_to_save}"
                file_with_path=f"{self.data_dir}{file_to_save}"
                downloaded_text = await resp.text()
        
        except Exception as e:
            print(f"Error {e} on url {url}")
            asyncio.sleep(1)
            raise e

        #give the other downloads a chance to be initialized.
        await asyncio.sleep(0)
        # small file, just read it all at once from the web,
        # save it to disk and read into the dataframe.
        to_read = StringIO(downloaded_text)
        async with aiofiles.open(file_with_path,"w") as f:
            await f.write(downloaded_text)
        return (file_to_save,self.process_futures_file(to_read,file_to_save))

    async def download_the_files(self,session,contract_list):
        #we just want the last two columns of contract_list
        #which will tbe the URL suffix and file name

        #when we are developing, we don't want to bring down the entire  set of files
        select_slice = slice(-max_download_files_for_development if limit_download_files_for_development else None,None)

        contracts_to_download =  contract_list if not limit_download_files_for_development else contract_list[-max_download_files_for_development:]

        contract_list = None   #so we don't accidentally reuse it

        #url_suffix is in the last column

        url_suffix = list(zip(*contracts_to_download))[-1:]
        #the last two columns are the url suffix and file name
        #y = ( (r[-2], r[-1]) for r in contracts_to_download )
        #prepend the url and file paths, and download
    #    logger.info(f"url_suffix[0] {url_suffix[0]}")

        z=( self.download_a_file(session,_data_root_url + r[-1], " Foo ") for  r in contracts_to_download)

        index_histories = await self.get_vix_index_histories(session)

        #tuples of file names and dataframes and 
        downloaded_tuples=(await asyncio.gather(*z))
        downloaded_frames = list(zip(*downloaded_tuples))[1]

        downloaded_frames = list(downloaded_frames)  + index_histories

        loop = asyncio.get_running_loop()
        df=await loop.run_in_executor(self._executor(),self.merge_the_frames,downloaded_frames)
        print(f"DF merged \n{df}")
        return df
    

  
    def process_futures_file(self,to_read,vix_futures_file_name):
        df=pd.read_csv(to_read,parse_dates=[0])
        future_contract_details=self.parse_vix_futures_file_name(vix_futures_file_name)
        (settlement_date_str,weekly, week_symbol_num_str,month_code,month_name,month_number,futures_year_string)=future_contract_details

        df['Frequency']="Weekly" if weekly else "Monthly"
        df['Week Number']=week_symbol_num_str
        if weekly:
            week_number = int(week_symbol_num_str)
        df['Month Code']=month_code
        df['Month Name']=month_name
        df['Month Number']=month_number
        df['Year']=futures_year = int(futures_year_string)
        df['Year']=df['Year'].astype(np.uint16)
        df['Symbol Root']=_monthly_first_token
        last_trade_date=df['Trade Date'].iloc[-1]

        today  = dt.date.today()
        today = pd.Timestamp.now()
        elapsed_since_last_trade = (today-last_trade_date).days 
     
 #       print(f"Elapsed since {last_trade_date} : {elapsed_since_last_trade} currently_trading {currently_trading}")
        df['Expiry']  = (settlement_date := pd.to_datetime(settlement_date_str))
        df['Tenor_Trade_Days']=((df['Expiry']-df['Trade Date']).dt.days).astype(np.int16)
       
        trade_days_to_settlement=pd.Series(index=df.index,dtype='int32')
        calculated_trade_days_to_settlement=pd.Series(index=df.index,dtype='int32')
        #determine the number of trade days remaining till
        #settlement
        trade_dates = df['Trade Date']
        settlement_date_local = pd.to_datetime(settlement_date).tz_localize('US/Eastern')
        for index, trade_date in trade_dates.iteritems():
            trade_date_local= pd.to_datetime(trade_date).tz_localize('US/Eastern')

            t1=time.time()
            exchange_open_days = self.valid_days.loc[trade_date_local:settlement_date_local]
            t2=time.time()
            calculated_trade_days_to_settlement.loc[index] = num_trade_days_to_settlement = len(exchange_open_days.array)
            t3=time.time()
        measured_trade_days_to_settlement=pd.Series(index=df.index,dtype='int32')
        trade_days=calculated_trade_days_to_settlement
        if last_trade_date == settlement_date:
            #determine the number of trade days based on trading 
            #history
            rows=len(df.index)
            measured_trade_days_to_settlement=pd.Series(index=df.index,data=range(rows-1,-1,-1))
            trade_days=measured_trade_days_to_settlement
        #these shoud be the same, but if there is a bug in the 
        #calendar, they will be different
        df.insert(0,'Measured Tenor_Days',measured_trade_days_to_settlement)
        df.insert(0,'Calculated Tenor_Days',calculated_trade_days_to_settlement)
        df.insert(0,"Tenor_Days",trade_days_to_settlement)
        df['File Name']=vix_futures_file_name
        #convert the columns that rarely changed to categories

        df['Year'].astype('int32',copy=False)
        for jj in _str_columns:
            df[jj]=df[jj].astype('string',copy=False)
        return df

    async def read_futures_file(self,vix_futures_file_name):
        vix_futures_file_path=f"{self.data_dir}/{vix_futures_file_name}"
        async with aiofiles.open(vix_futures_file_path, mode='r') as f:
            contents = await f.read()
        to_read = StringIO(contents)
        return to_read


    def parse_vix_futures_file_name(self,vix_futures_file_name):
        #monthly will look like 2014-09-17.CFE_VX.U4.csv
        #weekly will look like 2016-02-23.CFE_VX08.G6.csv

        (settlement_date_str,product_string,year_and_month_code,csv_str)=vix_futures_file_name.split(".")

        weekly = product_string != 'CFE_VX'
        week_symbol_num_str = vix_futures_file_name[17:19] if weekly else ''
        month_code = year_and_month_code[0]
        month_index = _futures_months_code.find(month_code)
        month_name = _futures_month_strings[month_index]
        month_number = _futures_month_numbers[month_index]
        #the year is the 2 characters after the month code

        futures_year_string = settlement_date_str[0:4]
   
        return ((settlement_date_str,weekly,week_symbol_num_str,month_code,month_name,month_number,futures_year_string))

    def merge_the_frames(self,read_frames):
        futures_frame=pd.concat(read_frames)
        for ii in _categorical:
            futures_frame[ii]=futures_frame[ii].astype('category',copy=False) 
        futures_frame.sort_index(ascending=True,inplace=True)
        return futures_frame

   

    def _add_nth_month(self,group):
        first_row=group.iloc[0]
        nearest_month = int( first_row[_dts] != 0)
        start_xmonth = first_row[_xm]
        group[_ym]=-start_xmonth+nearest_month
        return group


    def _build_continous_monthly_vix_futures(self,futures_df,min_month=1,max_month=8):
        #min_month of 0 will include futures on their Expiry
        # one trade date a month 
        # max_month > 9 will show futures with longer maturity
        # that may not be regularly issued.  Nov 2020 traded for a year.

        df=futures_df
        #drop the weekly data
        df2 = df[  df['Frequency']=='Monthly' ].sort_values(by=[_sd,_td])
        #drop anything with zero volume
        #CBOE data is suspect on days with no volume
        df3 = df2[df2["Total Volume"] !=0 ]
        

        #start at year 2000 so the MonthCount is easier to eyeball, 
        #it doesn't really matter which year is picked.
        #we are using it to figure out which month is the first, 2nd, etc.
        xmonth_start_year = 2000
        #XMonth - months elapsed since an arbitrary start date
        df3.insert(0,_xm,(df3[_sd].dt.year - xmonth_start_year)*12 +df3[_sd].dt.month)
        #YMonth is the XMonth+1 for the 1st expiring future, 
        # it will be XMonth+0 for a future expiring on the trade day.  
        df3.insert(0,_ym,-500)
        t0=time.time()
        for ii in [_ym,_xm]:
            df3[ii].astype('int32',copy=False)
        gb = df3.set_index([_td,_sd]).sort_index().groupby(_td)

        t1=time.time()
        #this takes a lot of time
        with_months=gb.apply(self._add_nth_month)
        t2=time.time()
        with_months.insert(0,_nth, with_months[_xm]+with_months[_ym])
        t3=time.time()

        filtered_lower_bound = with_months[with_months[_nth]>=min_month]
        filtered_upper_bound = filtered_lower_bound[filtered_lower_bound[_nth] <= max_month]

        result = filtered_upper_bound[_final_cols]
        return result


    async def get_vix_continous_futures(self,wide=True):
        t1=time.time()
        parquet_path=self.cf_parquet_path
        async with aiofiles.open(parquet_path, mode='rb') as f:
            raw_data = await f.read()
            contents = BytesIO(raw_data)
        t2=time.time()
        df = pd.read_parquet(contents)
        t3=time.time()
        print(f"read time {t2-t1}  load_time {t3-t2}")
        return self.unstack_futures(df) if wide else df

    def get_vix_continous_futures_sync(self,wide = True):
        loop = asyncio.get_event_loop()
        return  loop.run_until_complete(self.get_vix_continous_futures(wide))
    
    def unstack_futures(self,data_frame):
        t1=time.time()
        df= data_frame.reset_index().pivot(columns=_nth,index=_td,values='tenor')
        t2=time.time()
        print(f"Unstacking time {t2-t1}")
        return df

logger = logging.getLogger("areq")
logging.basicConfig(level=logging.INFO)




#an interesting url https://markets.cboe.com/us/futures/market_statistics/historical_data/products/

def elapsed():
    return t2-t1


def main():
    vixutil_path = user_path / ".vixutil"

    user_path = Path(vixutil_path)
    
    dl=VXFuturesDownloader(user_path)
    dl.get_contract_names_from_vix_full_history()
    extract_urls()

def not_main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    import argparse as ap
    parser=ap.ArgumentParser()
    file_choice_list=[pq:="parquet","csv","excel","html"]
    parser.add_argument('-n', action= 'store_true',help="""Do not update database before producing output. 
    Use the data already downloaded.  This is useful if you want multiple output files from the exact same underlying data. """)
    parser.add_argument('--data_dir',required=True,help='''A directory to store files.  Files
    downloaded from CFE as well as intermediate files produced by the underlying library
    are stored in this directory.  ''')
    data_frame_shape_list=[wide:="wide",(shape_defult:=(tall:='tall')),raw:="raw"]
    parser.add_argument("-shape",choices=data_frame_shape_list,help="""
    Specify a wide or tall dataframe for continuous futures, or raw to get the
    data from CFE for all contracts aggregated into a single dataframe, but otherwise unaltered. The raw
    shape also includes weekly contracts. 
    Select tall for having the row values indexed by Trade Date and Tenor.  This is the more usable format
    if you can unstack or pivot the data in your code.     
    A wide dataframe will have a column
    for each data series (i.e. 'Close', 'Tenor_Days'), for each monthly tenor.  For the parquet or CSV format,
    in the wide dataframe the monthly tenors are concatenated to the column name, so you get "Close_1", "Close_2".  
    The  default is {shape_default}.  Select tall if there are facilities available to pivot or unstack 
    the data in your target environment.  """, default=wide)
    parser.add_argument('-f',default=pq,choices=file_choice_list,help=f"""
    File format to produce.  '{pq}'' is the default and most efficient of the
    options.    
    """)
    column_choices=[def_cols:="normal","verbose","raw"]
    parser.add_argument("-c",default=def_cols,choices=column_choices,help="""
    The default 'normal' choice columns has the close value and days to  settlment  for each tenor.   
    Verbose columns contains more columns including the futures symbol for each tenor.   
     """)

    parser.add_argument("-o",help="""file name to save the data frame to.  If no file is specified, write to standard output.  """)
    local_data_dir="C:/Users/Dougr/OneDrive/family/doug/work in progress/python projects/gatherVix/cboe_data/"
    futures = VXFutures(local_data_dir)
    futures.get_data_sync()
#    df,df2,df3 = tuple(futures.get_data_sync() for i in range(3))
 #   print(f"df {df} df.info {df.info()}")

    #the code to flatten the column names for wide frames
#    cols = [f"{c}_{t}" for (c, t) in df.columns]
#    df2 = df.copy()
#    df2.columns = pd.Index(cols)

    sys.exit(0)


if __name__ == "__main__":
    main()