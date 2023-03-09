# VIX Utils
## Overview

*vix_utils* provides some tools for preparing data for analysing  the VIX Futures and Cash Term structures.

VIX Futures Data downloaded from [CBOE Futures Historical Data](https://www.cboe.com/us/futures/market_statistics/historical_data/).

Vix Cash Data are downloaded from [CBOE Historical Volatility Indexes](https://www.cboe.com/tradable_products/vix/vix_historical_data/).


There is an API for Python to load the data into Pandas DataFrames.  If you do your analysis in Python, use the API.

Since there is no documentation yet, look at the examples in the src/vix_utils/examples folder.

If you do your analysis in other tools such as R or excel, you can use the command line tool vixutil.

`vixutil -h` will give the help.  The data are availble in record and wide formats.  Just run it and look at the excel or csv output to see what they look like.

## Coming Soon
30 day continuous maturity weighting of front two months of vix futures.

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

When you run the samples, they will print out the Python script file names so you can find them wherever pip installs them.

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
## Developing
https://numpydoc.readthedocs.io/en/latest/format.html
