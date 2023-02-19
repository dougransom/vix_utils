import aiohttp
import asyncio
import aiofiles
from appdirs import user_data_dir,user_log_dir
from pathlib import Path
import itertools
import vix_futures_term_struture as t
import datetime as dt
vix_futures_settlement_date_monthly=t.vix_futures_settlement_date_monthly

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

          
def generate_monthly_url(year,month):
    settlement_date=vix_futures_settlement_date_monthly(year,month)
    settlement_date_str=settlement_date.isoformat()[:10]
    url=f"https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_{settlement_date_str}.csv"     
    return url,settlement_date_str

def generate_monthly_urls():
     return (generate_monthly_url(y,m) for y,m in years_and_months)

async def dump_to_file(fn,data):
        try:
            async with aiofiles.open(fn,mode="wb") as f:
                await f.write(data)
        except Exception:
            async with aiofiles.open(fn,mode="w") as f:
                await f.write(data)




class VXFuturesDownloader:
    historical_data_url="https://www.cboe.com/us/futures/market_statistics/historical_data/"
    def __init__(self,data_dir):

        #folder to store data files
        self.data_dir = data_dir
        self.futures_data=data_dir/"futures"
        p=self.futures_data_cache=data_dir/"futures"/"download"
        p.mkdir(exist_ok=True,parents=True) 
 
    async def download_one_future(self,year,month):
        url,expiry=generate_monthly_url(year,month)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status !=  200:
                    return 
                headers=response.headers
                print(f"Headers {headers}")
                content_disposition=headers[_cc]
                cboe_filename= content_disposition.split('"')[-2]   

                file_to_save = f"{expiry}.{cboe_filename}"
                file_with_path=self.futures_data_cache/file_to_save
                response_data=await response.read()
                print(f"Text response \n{response_data}")
                await dump_to_file(file_with_path, response_data)
                
    async def download_futures(self):
        futures_to_download=(self.download_one_future(y,m) for (y,m) in years_and_months())
        await asyncio.gather(*futures_to_download)

    async def download_history_root(self):


            async with aiohttp.ClientSession() as session:

                async with session.get(self.historical_data_url) as response:
                     history_file=self.futures_data_cache/"history.html"
                     history_headers_file=self.futures_data_cache/"history.txt"
                     async with asyncio.TaskGroup() as tg:
                          headers=f"{response.headers}"
                          tg.create_task(dump_to_file(history_headers_file,headers))
                          text=await response.read()
                          tg.create_task(dump_to_file(history_file,text))

                return text     

         

    
async def main():
    user_path = Path(user_data_dir())
    vixutil_path = user_path / ".vixutil"
    vixutil_path.mkdir(exist_ok=True)
    v=VXFuturesDownloader(vixutil_path)
    await v.download_history_root()

    u=generate_monthly_url(2023,1)
    await v.download_futures()
    print(f"U {u}")

    
asyncio.run(main())    