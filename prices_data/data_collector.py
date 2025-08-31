import requests
import pandas as pd
from sqlalchemy import create_engine

def get_bitcoin_price():
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    response = requests.get(url)
    data = response.json()
    print(f"BTC Price: ${data['price']}")
    return float(data['price'])
y=0
x=100
for i in range(x):
    print(f"Number:{y}-------{get_bitcoin_price()}")
    y+=1
    if y==70:
        break
