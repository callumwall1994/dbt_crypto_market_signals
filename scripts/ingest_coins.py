import requests
from databricks import sql
from dotenv import load_dotenv
import os

#Load local variables
load_dotenv()

CG_API_KEY= os.getenv("CG_API_KEY")

url="https://api.coingecko.com/api/v3/coins/markets"
params={
    "vs_currency":"usd",
    "price_change_percentage":"1h",
    "include_tokens":"top",
    "order":"market_cap_desc",
    "per_page":"3"
}

header= {"x-cg-demo-api-key": CG_API_KEY}

response= requests.get(url, headers=header, params=params)
print(response.status_code)

data=response.json()

##Connection details
connection= sql.connect(
    server_hostname= os.getenv("SERVER_HOSTNAME"),
    http_path= os.getenv("HTTP_PATH"),
    access_token=os.getenv("ACCESS_TOKEN")
)

cursor= connection.cursor()

#Create Table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS crypto_raw (
               id STRING,
               name STRING,
               current_price DOUBLE,
               market_cap DOUBLE,
               market_cap_rank INT,
               total_volume DOUBLE,
               high_24h DOUBLE,
               low_24h DOUBLE,
               last_updated TIMESTAMP
)        
""")

#Insert rows
for item in data:
    cursor.execute("""
        INSERT INTO crypto_raw (id, name, current_price, market_cap, market_cap_rank, total_volume, high_24h, low_24h, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        item["id"],
        item["name"],
        item["current_price"],
        item["market_cap"],
        item["market_cap_rank"],
        item["total_volume"],
        item["high_24h"],
        item["low_24h"],
        item["last_updated"]
    ))
    
connection.close()

print("Success!")
