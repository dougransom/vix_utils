import pandas as pd

df=pd.read_pickle("C:/Users/doug/AppData/Local/.vixutil/skinny.pkl")
weekly=df[df['Weekly']] 
monthly=df[df['Weekly'] == False]