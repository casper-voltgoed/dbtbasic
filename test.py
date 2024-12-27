import numpy as np
import pandas as pd


df = pd.DataFrame({'float': [1.0], 'int': [1], 'datetime': [pd.Timestamp('20180310')], 'string': ['foo']})

x = df.dtypes
for xtype in x:
    print(xtype, np.issubdtype(xtype, np.integer))


for col in df:
    dtype = df[col].dtype

    print(df[col].dtype)
