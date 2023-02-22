import pandas as pd

df=pd.read_pickle("C:/Users/doug/AppData/Local/.vixutil/skinny.pkl")
weekly=df[df['Frequency']=='Weekly'] 
monthly=df[df['Frequency']=='Monthly']
