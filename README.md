# VIX Utils
## Overview

*vix_utils* provides some tools for preparing data for analysing  the VIX Futures and Cash Term structures.

It provides a command line program that:

* generates a mapping of trade dates to the next settlement date for each vix future maturity.
* downloads the VIX Futures Data from [Quandl](https://www.quandl.com/) and puts
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

`vixutils -r -t futures_term_structure.xlsx -c cash_term_structure.json `
 
For a detailed command line arguments, run 

`vixutils -h`

## Calling from Python

IF you are using Python, it is still easiest to use the command line tool to download the data.

`vixutils -r`.

Then import vixutils into your python program.

`import vixutils`

Look in vixutils.py and it should be fairly obvious how to request the 
data you would like.



## Installation

You will need a Python 3.9 or later instalation.

### Install from the Python Packaging Index
From an elevated command prompt, 
install using pip from [The Python Package Index ](https://www.pypi.org):

`pip install vix_utils"

### Installing after cloning the Git Repository

From an elevated command prompt,
`flit install --symlink` will create the command line program in your python Scripts directory and symlink the 
appropriate files in SitePackages to your repository.

## Example

An example Python program to use the API to plot the 
term structure is in example_plot_vix_term_structure.py



