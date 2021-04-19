import os
from os.path import join, dirname
from dotenv import load_dotenv
import pandas as pd
import pymongo
from math import sqrt


def load_envs():
    dotenv_path = join(dirname(__file__), ".env")
    load_dotenv(dotenv_path)


def get_rates():
    driver = pymongo.MongoClient(os.environ.get("DB_CONNECT"))
    db = driver[os.environ.get("DB")]
    col = db[os.environ.get("COL")]
    data = col.find({}, {"_id": 0, "date": 0, "__v": 0})
    rates_df = pd.DataFrame(list(data))

    return rates_df


def groupByUsers_sorted(new_rates_df, id):
    filtergames_df = new_rates_df.loc[rates_df["idUser"] == id]
    new_rates_df = new_rates_df[new_rates_df.idUser != id]
    new_rates_df = new_rates_df[new_rates_df["idGame"].isin(
        filtergames_df["idGame"].tolist())]
    user_dt = new_rates_df.groupby(["idUser"])
    user_dt = sorted(user_dt, key=lambda x: len(x[1]), reverse=True)
    user_dt = user_dt[0:100]

    return user_dt


def get_idUsers(rates_df):
    rates_df = rates_df.drop_duplicates('idUser', keep='last').drop(
        columns=["score", "idGame", "nameGame"])

    idUsers_list = rates_df["idUser"].tolist()

    return idUsers_list


def pearson_correlation_df(users_list, user_rate):
    pearson_dic = {}

    for name, group in users_list:
        group = group.sort_values(by="idGame")
        user_rate = user_rate.sort_values(by="idGame")

        nRates = len(group)
        temp_df = user_rate[user_rate["idGame"].isin(
            group["idGame"].tolist())]
        temp_list = temp_df["score"].tolist()
        temp_group_list = group["score"].tolist()

        userA = sum([i**2 for i in temp_list]) - \
            pow(sum(temp_list), 2)/float(nRates)

        userB = sum([i**2 for i in temp_group_list]) - \
            pow(sum(temp_group_list), 2)/float(nRates)

        userAB = sum(i*j for i, j in zip(temp_list, temp_group_list)) - \
            sum(temp_list) * sum(temp_group_list) / float(nRates)

        if userA != 0 and userB != 0:
            pearson_dic[name] = userAB/sqrt(userA * userB)
        else:
            pearson_dic[name] = 0

    pearson_df = pd.DataFrame.from_dict(pearson_dic, orient="index")
    pearson_df.columns = ["simIndex"]
    pearson_df["idUser"] = pearson_df.index
    pearson_df.index = range(len(pearson_df))

    return pearson_df


def game_recommendation(pearson_df, rates_df):
    topUsers = pearson_df.sort_values(by="simIndex", ascending=False)[0:10]
    topUsersRated = topUsers.merge(
        rates_df, left_on="idUser", right_on="idUser", how="inner")

    topUsersRated["weighRated"] = topUsersRated["simIndex"] * \
        topUsersRated["score"]

    tempSumRatings = topUsersRated.groupby(
        "nameGame").sum()[["simIndex", "weighRated"]]

    tempSumRatings.columns = ["sumSimIndex", "sumWeighRated"]

    recommendation_df = pd.DataFrame()

    recommendation_df["weight avg rec score"] = tempSumRatings["sumWeighRated"] / \
        tempSumRatings["sumSimIndex"]

    recommendation_df["nameGame"] = tempSumRatings.index

    recommendation_df = recommendation_df.sort_values(
        by="weight avg rec score", ascending=False)[0:10]

    print(recommendation_df)


if __name__ == "__main__":
    load_envs()
    rates_df = get_rates()
    idUsers_list = get_idUsers(rates_df)

    for id in idUsers_list:
        print("\niteration id: " + id + "\n")
        users_list = groupByUsers_sorted(rates_df, id)
        pearson_df = pearson_correlation_df(
            users_list, rates_df.loc[rates_df["idUser"] == id])

        game_recommendation(pearson_df, rates_df)
