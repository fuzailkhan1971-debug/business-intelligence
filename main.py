from fastapi import FastAPI
import pandas as pd
import sqlite3
from dotenv import load_dotenv #hidden key loading 
import os #reads API key
from google import genai#gemini
from fastapi import UploadFile,File



load_dotenv("flash.env")
api_key=os.getenv("GEMINI_API_KEY")

client=genai.Client(api_key=api_key)


app=FastAPI()

new=sqlite3.connect("amazon_sales.db",check_same_thread=False)


def run_query(sql_query):
    try:
        print("RUNNING SQL",sql_query)
        result = pd.read_sql_query(sql_query, new)
        result = result.where(pd.notnull(result), None)
        return result.to_dict(orient="records")
    except Exception as e:
        print("SQL ERROR:",e)
        return {"error":str(e)}


def generate_sql(question):
    prompt = f"""
Convert this question into a valid SQLite SQL query.

Table: sales

Columns:
order_date, product_id, product_category, price,
discount_percent, quantity_sold, customer_region,
payment_method, rating, review_count

Rules:
- Only SELECT
- No explanation
- No ```sql
- Single line SQL
- Use exact column names
- Always alias aggregations with AS (e.g. SUM(...) AS total_revenue)
- Q1 = Jan-Mar, Q2 = Apr-Jun, Q3 = Jul-Sep, Q4 = Oct-Dec  
- order_date format is YYYY-MM-DD  

Question: {question}
"""

    response = client.models.generate_content(
        model="models/gemini-2.5-flash",
        contents=prompt
    )

    sql = response.text or ""

    sql = sql.lower()
    sql = sql.replace("```sql", "").replace("```", "")
    sql = sql.replace("\n", " ")
    sql = sql.replace(";", "")
    sql = " ".join(sql.split())

    if not sql.startswith("select") or "from sales" not in sql:
        raise Exception("Invalid sql generated")

    return sql

    

@app.get("/")
def home():
    return {"message":"sales query API running"}


@app.post("/ask")
def ask(question: str):
    try:
        sql_query = generate_sql(question)
        print("SQL:", sql_query)

        result = run_query(sql_query)

        return {
            "question": question,
            "sql": sql_query,
            "data": result
        }

    except Exception as e:
        print("ERROR:", e)
      #fallback
        safe_sql ="""SELECT product_category, SUM(price * quantity_sold) as total_revenue
        FROM sales GROUP BY product_category"""
        result = run_query(safe_sql)

        return {
            "question": question,
            "sql": safe_sql,
            "data": result,
            "note": "fallback used"
        }



@app.post("/upload")
def upload(file:UploadFile=File(...)):
    df=pd.read_csv(file.file,encoding="latin1")
    df.to_sql("sales",new,if_exists="replace",index=False)
    return {"message":"Dataset uploaded successfully"}

@app.get("/columns")
def get_columns():
    cursor = new.execute("PRAGMA table_info(sales)")
    columns = [row[1] for row in cursor.fetchall()]
    return {"columns": columns}


@app.get("/loadcsv")
def load_csv():
    df = pd.read_csv("amazon_sales.csv", encoding="latin1")
    df = df.dropna(how="all")
    df.to_sql("sales", new, if_exists="replace", index=False)
    return {"message": "loaded", "columns": list(df.columns)}