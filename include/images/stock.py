import yfinance as yf
import pickle
from tqdm import tqdm
import time

for i in tqdm(range(20)):
    time.sleep(0.5)


tsla = yf.Ticker("TSLA")

print(tsla)
