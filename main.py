from fastapi import FastAPI
import pandas as pd
import sqlite3

app=FastAPI()

new=sqlite3.connect("amazon_sales.db",check_same_thread=False)

def run_query(sql_query):
    result = pd.read_sql_query(sql_query,new)
    return result.to_dict(orient="records")

@app.get("/")
def home():
    return {"message":"sales query API running"}

@app.post("/query")
def query_databse(sql_query: str):
    result=run_query(sql_query)
    return {"data":result}