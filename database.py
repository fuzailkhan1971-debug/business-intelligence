import pandas as pd

df =pd.read_csv("amazon_sales.csv", encoding="latin1", skiprows=1)
df=df.iloc[:, 1:] #remove the first column 
print(df.head())
print(df.columns)

import sqlite3 #to create a dataset in SQL named sales

new=sqlite3.connect("amazon_sales.db")
#creating a table in SQL
df.to_sql("sales",new,if_exists="replace",index=False)
print("Database created successfully")

def run_query(sql_query):
    result = pd.read_sql_query(sql_query, new)
    return result

query = """
SELECT product_category, SUM(total_revenue)
FROM sales
GROUP BY product_category
"""
result=run_query(query)
print(result)

new.close()#close the connection to the database


