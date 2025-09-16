import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from data_cleaning import clean_sales_data
from weather_client import get_weather

load_dotenv()

DB_NAME = os.getenv("DB_NAME", "retaildb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        host=DB_HOST, port=DB_PORT
    )

def load_weather(cur, store_id, lat, lon):
    weather = get_weather(lat, lon)
    cur.execute("""
        INSERT INTO dim_weather (store_id, weather_date, temp_c, humidity, weather_main, weather_description, wind_speed)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        RETURNING weather_id;
    """, (store_id, weather["weather_date"], weather["temp_c"], weather["humidity"],
          weather["weather_main"], weather["weather_description"], weather["wind_speed"]))
    return cur.fetchone()[0]

def run_etl():
    conn = get_connection()
    cur = conn.cursor()

  
    df, dupes, missing = clean_sales_data("data/raw_sales.csv")


    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO dim_store (store_id, store_name, store_city, store_state, lat, lon)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (store_id) DO NOTHING;
        """, (row["store_id"], row["store_name"], row["store_city"], row["store_state"], row["lat"], row["lon"]))

        cur.execute("""
            INSERT INTO dim_product (product_id, product_name, category)
            VALUES (%s,%s,%s)
            ON CONFLICT (product_id) DO NOTHING;
        """, (row["product_id"], row["product_name"], row["category"]))

        cur.execute("""
            INSERT INTO dim_customer (customer_id, customer_name, customer_segment)
            VALUES (%s,%s,%s)
            ON CONFLICT (customer_id) DO NOTHING;
        """, (row["customer_id"], row["customer_name"], row["customer_segment"]))


    sales_data = []
    for _, row in df.iterrows():
        weather_id = load_weather(cur, row["store_id"], row["lat"], row["lon"])
        sales_data.append((
            row["sale_id"], row["sale_date"], row["store_id"], row["customer_id"],
            row["product_id"], row["qty"], row["price"], row["total"],
            row["payment_method"], weather_id
        ))

    execute_values(cur, """
        INSERT INTO fact_sales (sale_id, sale_date, store_id, customer_id, product_id, qty, price, total, payment_method, weather_id)
        VALUES %s
        ON CONFLICT (sale_id) DO NOTHING;
    """, sales_data)

    
    cur.execute("""
        INSERT INTO etl_audit (source_file, rows_in, rows_out, rows_rejected, notes)
        VALUES (%s,%s,%s,%s,%s)
    """, ("data/raw_sales.csv", len(df)+dupes+missing, len(df), dupes+missing, "ETL run completed."))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ ETL completed successfully!")

if __name__ == "__main__":
    run_etl()
