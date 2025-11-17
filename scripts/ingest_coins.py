import requests
from databricks import sql
from dotenv import load_dotenv
import os
from datetime import datetime, timezone
import json

#Load local variables
load_dotenv()

#CoinGecko API
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
    CREATE TABLE IF NOT EXISTS crypto_bronze_raw (
               raw_data STRING,
               created_at TIMESTAMP
               
)        
""")

#Insert rows with data
created_at_timezone= datetime.now(timezone.utc)
created_at= created_at_timezone.strftime('%Y-%m-%dT%H:%M:%S.%f')

for coin in data:
    raw_json=json.dumps(coin)

    cursor.execute("""
        INSERT INTO crypto_bronze_raw (raw_data, created_at)
        VALUES (?, ?)
    """, (raw_json, created_at))
    
connection.close()

print(f"Top coins added in as of {datetime.now()}")
