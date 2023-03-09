# VIX Utils
## Overview

*vix_utils* provides some tools for preparing data for analysing  the VIX Futures and Cash Term structures.

Currently, only the functions to download the vix futures and cash term structures work.
See the notes about examples below.   

The  virtual 30 day future used by VXX and the SVXSTR index are not working yet. Skip down to [installation](#installation). 
~~~
TODO the command line program is not working
It provides a command line program that:

* generates a mapping of trade dates to the next Expiry for each vix future maturity.
* downloads the VIX Futures Data from [CBOE](https://www.cboe.com/us/futures/market_statistics/historical_data/) and puts
all the maturities in a row for a given trade date.  
* downloads the cash  Vix Term Structure and produces a table indexed by trade date.
other CBOE index are included.
* prepares a continuous maturity VIX Futures term structure.  For example, instead of the front month, and second month maturities, it calculates a point on the term structure maturing in one month, two month, etc. from the trade date.  This can also be useful to approximate the [S&P 500 VIX Short-Term Futures Index](https://www.spglobal.com/spdji/en/indices/strategy/sp-500-vix-short-term-index-mcap/#overview) or Exchand Traded Products like [VXX](https://www.ipathetn.com/US/16/en/details.app?instrumentId=341408) or [UVXY](https://www.proshares.com/funds/uvxy.html).

The output can be saved in a variety of formats that can be imported into common analysis tools:

* .csv (comma seperated values)
* .pkl  (python pickle format)
*  .xlsx (excel)
*  .html

If Python is your language of choice, there is also an API available that can load the data into
Pandas dataframes.

## Sample command line

This will download the data from CBOE and Quandl, and save the futures and cash term structures as 
Microsoft Excel Files.

`vixutils -r -t futures_term_structure.xlsx -c cash_term_structure.xlsx `
 
For a detailed command line arguments, run 

`vixutils -h`

## Calling from Python

IF you are using Python, it is still easiest to use the command line tool to download the data.

`vixutils -r`.

Then import vixutils into your python program.

`import vixutils`

Look in vixutils.py and it should be fairly obvious how to request the 
data you would like.

~~~

## Installation

You will need a Python 3.11 or later instalation.

### Install from the Python Packaging Index
 
Install using pip from [The Python Package Index ](https://www.pypi.org):

`pip install vix_utils`

If you want to run the samples, install like this:
`pip install vix_utils[examples]`

The sample to load all the various data frames can be run as:
'vix_loadframes'

The sample to plot the history of futures and cash term structures:
`vix_sample_plots`

### Development or Running the Samples

Clone from  [github repository](https://github.com/dougransom/vix_utils).

 
`pip install -e .[test,examples]` will:
- install vix_utils into your python environment, including any command line scripts. 
- install the necessary prequisites for running any 
tests in the `test` folder, and for running the programs in the `examples` folder.

#### Testing

The tests directory contains a few tests.  This project wasn't developed with 
[Test Driven Development](https://www.agilealliance.org/glossary/tdd/), unit tests have been added to isolate
and fix defects. 

However, new features and bug fixes should be developed with [Test Driven Development](https://www.agilealliance.org/glossary/tdd/) practices when practical.


## Examples
In the examples folder:
* `a_loadframes.py` shows how to load all the data for vix futures and vix cash histories.
* `c_plot_vix_term_structure.py` will plot the vix futures history and the vix cash history.
 
~~~
## Developing
https://numpydoc.readthedocs.io/en/latest/format.html
