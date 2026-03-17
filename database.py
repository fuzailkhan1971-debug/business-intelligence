import pandas as pd
import sqlite3

df =pd.read_csv("amazon_sales.csv", encoding="latin1", skiprows=1)
df=df.iloc[:, 1:] #remove the first column 
df.columns=df.columns.str.strip()
print(df.head())
print(df.columns)

import sqlite3 #to create a dataset in SQL named sales

new=sqlite3.connect("amazon_sales.db",check_same_thread=False)
#creating a table in SQL
df.to_sql("sales",new,if_exists="replace",index=False)
print("Database created successfully")

new.close()