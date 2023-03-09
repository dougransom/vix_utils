

#from 
https://datashop.cboe.com/cfe-vix-volatility-index-futures-trades-quotes

* Prior to 3/23/2007, VIX had a $100x multiplier. On 3/26/2007 we changed this multiplier to $1000x and divided the display price by 10. https://cdn.cboe.com/resources/regulation/circulars/general/CFE-IC-2007-003.pdf


#some of the archived data (2004-2005) has some erroneus 
#date which appear as duplicates.
```

                       Weekly  Tenor_Days  Tenor_Trade_Days  ...     Futures                              File  Expired
Trade Date Tenor_Monthly                                                        ...
2004-05-19 1.0          False                       0.0                  -1  ...  K (May 04)   2004-05-18.m_5.CFE_VX_K2004.csv    False
           1.0          False                      17.0                  27  ...  M (Jun 04)   2004-06-15.m_6.CFE_VX_M2004.csv    False
2004-06-16 1.0          False                       0.0                  -1  ...  M (Jun 04)   2004-06-15.m_6.CFE_VX_M2004.csv    False
           1.0          False                      23.0                  34  ...  N (Jul 04)   2004-07-20.m_7.CFE_VX_N2004.csv    False
2004-08-18 1.0          False                       0.0                  -1  ...  Q (Aug 04)   2004-08-17.m_8.CFE_VX_Q2004.csv    False
           1.0          False                      18.0                  27  ...  U (Sep 04)   2004-09-14.m_9.CFE_VX_U2004.csv    False
2004-09-15 1.0          False                       0.0                  -1  ...  U (Sep 04)   2004-09-14.m_9.CFE_VX_U2004.csv    False
           1.0          False                      24.0                  34  ...  V (Oct 04)  2004-10-19.m_10.CFE_VX_V2004.csv    False
2005-01-19 1.0          False                       0.0                  -1  ...  F (Jan 05)   2005-01-18.m_1.CFE_VX_F2005.csv    False
           1.0          False                      19.0                  27  ...  G (Feb 05)   2005-02-15.m_2.CFE_VX_G2005.csv    False
2005-02-16 1.0          False                       0.0                  -1  ...  G (Feb 05)   2005-02-15.m_2.CFE_VX_G2005.csv    False
           1.0          False                      18.0                  27  ...  H (Mar 05)   2005-03-15.m_3.CFE_VX_H2005.csv    False

[12 rows x 18 columns]
>>>
```
```
        # #july-nov 2013 need to be fixed up by removing the first row.
        # cache_dir=vixutil_path/"futures"/"download"/"archive_monthly"
        # to_fix=[
        # "2013-07-17.m_7.CFE_VX_N2013.csv",
        # "2013-08-21.m_8.CFE_VX_Q2013.csv",
        # "2013-10-16.m_10.CFE_VX_V2013.csv",
        # "2013-11-20.m_11.CFE_VX_X2013.csv"]
        ```
        
Some more weirdness addressed by removing andy rows with a Close value of 0

```
WARNING:root:
******************************Duplicates in index:
                       Weekly  Tenor_Days  Tenor_Trade_Days  ...     Futures                             File  Expired        
Trade Date Tenor_Monthly                                                        ...
2006-03-22 12.0         False                     226.0                 329  ...  G (Feb 07)  2007-02-14.m_2.CFE_VX_G2007.csv     True        
           12.0         False                     289.0                 419  ...  K (May 07)  2007-05-16.m_5.CFE_VX_K2007.csv     True        
2006-06-01 12.0         False                     240.0                 349  ...  K (May 07)  2007-05-16.m_5.CFE_VX_K2007.csv     True        
           12.0         False                     496.0                 720  ...  K (May 08)  2008-05-21.m_5.CFE_VX_K2008.csv     True        
2006-06-02 12.0         False                     239.0                 348  ...  K (May 07)  2007-05-16.m_5.CFE_VX_K2007.csv     True        
...                       ...                       ...                 ...  ...         ...                              ...      ...        
2007-03-20 12.0         False                     296.0                 428  ...  K (May 08)  2008-05-21.m_5.CFE_VX_K2008.csv     True        
2007-03-21 12.0         False                     230.0                 335  ...  G (Feb 08)  2008-02-19.m_2.CFE_VX_G2008.csv     True        
           12.0         False                     295.0                 427  ...  K (May 08)  2008-05-21.m_5.CFE_VX_K2008.csv     True        
2007-06-20 12.0         False                     232.0                 336  ...  K (May 08)  2008-05-21.m_5.CFE_VX_K2008.csv     True        
           12.0         False                     251.0                 364  ...  M (Jun 08)  2008-06-18.m_6.CFE_VX_M2008.csv     True        
```