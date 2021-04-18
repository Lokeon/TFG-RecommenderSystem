import os
from os.path import join, dirname
from dotenv import load_dotenv
import pandas as pd
import pymongo


def load_envs():
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)


def get_rates():
    driver = pymongo.MongoClient(os.environ.get("DB_CONNECT"))
    db = driver[os.environ.get("DB")]
    col = db[os.environ.get("COL")]
    data = col.find({}, {'_id': 0, "date": 0, "__v": 0})
    rates_df = pd.DataFrame(list(data))
    # print(rates_df)


if __name__ == "__main__":
    load_envs()
    get_rates()
