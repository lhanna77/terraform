from fastapi import FastAPI
from pandas import read_csv
import redis

# reading CSV file
data_dl = read_csv('./src/data.csv')

# converting column data to list
comma_sep_list = data_dl['list'].tolist()

app = FastAPI()

r = redis.Redis(host="redis", port=6379)

@app.get("/")
def read_root():
    return{"your comma sep list": comma_sep_list}

@app.get("/hits")
def read_root():
    r.incr("hits")
    return{"Hits": r.get("hits")}