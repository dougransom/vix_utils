# VIX Utils
## Overview

*vix_utils* provides some tools for preparing data for analysing  the VIX Futures and Cash Term structures.

The futures can also contain a 30 day continuous maturity weighting of front two months of vix futures.

VIX Futures Data downloaded from [CBOE Futures Historical Data](https://www.cboe.com/us/futures/market_statistics/historical_data/).

Vix Cash Data are downloaded from [CBOE Historical Volatility Indexes](https://www.cboe.com/tradable_products/vix/vix_historical_data/).


There is an API for Python to load the data into Pandas DataFrames.  If you do your analysis in Python, use the API.

Since there is no documentation yet, look at the examples in the src/vix_utils/examples folder.
There is a Jupyter Notebook vix_utils.ipynb in that folder.

*Important note for Juypter notebooks.*  
You must use  async_get_vix_index_histories and async_load_vix_term_structure 
rather than get_vix_index_histories and load_vix_term_structure.  There is an example Jupyter notebook "vix_utils use in Jupyter.ipynb" in the src/vix_utils/examples folder. 
 
If you do your analysis in other tools such as R or excel, you can use the command line tool vixutil.

`vixutil -h` will give the help.  The data are availble in record and wide formats.  Just run it and look at the excel or csv output to see what they look like.

 


## Installation

You will need a Python 3.11 or later instalation.

### Install from the Python Packaging Index
 
Install using pip from [The Python Package Index ](https://www.pypi.org):

`pip install vix_utils`

If you want to run the samples, install like this:
`pip install vix_utils[examples]`

The sample to load all the various data frames can be run as:
'vix_sample_load_data'

The sample to plot the history of futures and cash term structures:
`vix_sample_plots`

To load the sample Jupyter notebook, run vix_sample_load_data to figure out where the examples folder is. Browse there with Jupyter and open a notebook.   


### Development 

Clone from  [github repository](https://github.com/dougransom/vix_utils).

 
`pip install -e .[test,examples]` will:
- install vix_utils into your python environment, including any command line scripts. 
- install the necessary prequisites for running any 
tests in the `test` folder, and for running the programs in the `src/vixutils/examples` folder.

#### Testing

The tests directory contains a few tests.  This project wasn't developed with 
[Test Driven Development](https://www.agilealliance.org/glossary/tdd/), unit tests have been added to isolate
and fix defects. 

However, new features and bug fixes should be developed with [Test Driven Development](https://www.agilealliance.org/glossary/tdd/) practices when practical.


## Examples
Source is in `src/vix_utils/examples`
 
~~~
## Data Notes
These dates appear to be missing from the CBOE Data.
At some point they need to be patched in if they exist.
```
[Timestamp('2006-11-10 00:00:00'), Timestamp('2007-01-03 00:00:00'), Timestamp('2021-04-02 00:00:00'), Timestamp('2021-12-24 00:00:00')]
```
There seem to be  a few dates where spot indexes are missing, you will have to workaround by using fill feature of Pandas datafame, or skip those days, in any analysis.
~~~
## Developing
https://numpydoc.readthedocs.io/en/latest/format.html

