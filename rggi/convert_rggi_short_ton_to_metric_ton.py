
# RGGI values as given by rggi.org are in units of short tons.  ReEDS considers rggicon.csv to have metric tons so we need to convert short tons to metric tons. 

import pandas as pd

# Read in rggicon file that has units of short tons 
df_short_ton = pd.read_csv('rggicon_short_tons.csv', header=None)

# Convert short ton to metric ton
df_metric_ton = df_short_ton.copy()
df_metric_ton.iloc[:,1] = (df_short_ton.iloc[:,1]*0.907).astype(int)

# Write to rggicon.csv 
df_metric_ton.to_csv('rggicon.csv', index=False, header=False)


# Test read csv
df = pd.read_csv('rggicon.csv', header=None)
print(df)