import pandas as pd

df=pd.read_pickle("C:/Users/doug/AppData/Local/.vixutil/skinny.pkl")
weekly=df[df['Frequency']=='Weekly'] 
monthly=df[df['Frequency']=='Monthly']
m2023=monthly[monthly['Trade Date'] > '2023-01-01']
val=m2023[m2023['Trade Date']=='2023-02-14']