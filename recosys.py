import os
from os.path import join, dirname
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import pymongo
import json
from math import sqrt


def load_envs():
    dotenv_path = join(dirname(__file__), ".env")
    load_dotenv(dotenv_path)


def get_rates():
    driver = pymongo.MongoClient(os.environ.get("DB_CONNECT"))
    db = driver[os.environ.get("DB")]
    col = db[os.environ.get("COL")]
    col2 = db[os.environ.get("COL2")]
    col2.delete_many({})
    print("Recommendation collection is wiped!!")
    data = col.find({}, {"_id": 0, "date": 0, "__v": 0})
    rates_df = pd.DataFrame(list(data))

    return rates_df


def groupByUsers_sorted(rates_df, user_rec, new_rates_df):
    # print("JUEGOS DEL USUARIO")
    # print(user_rec)
    # all games from other users that are the same as the recmed user
    user_dt = new_rates_df[new_rates_df["idGame"].isin(
        user_rec["idGame"].tolist())]
    # print("JUEGOS IGUALES AL DEL USUARIO")
    # print(user_dt)
    user_dt_subgroup = user_dt.groupby(["idUser"])
    user_dt_subgroup = sorted(
        user_dt_subgroup, key=lambda x: len(x[1]), reverse=True)
    user_dt_subgroup = user_dt_subgroup[0:100]

    return user_dt_subgroup


def get_user_list(rates_df, id):
    user_rec = rates_df.loc[rates_df["idUser"] == id]

    return user_rec


def get_idUsers(rates_df):
    rates_df = rates_df.drop_duplicates('idUser', keep='last').drop(
        columns=["score", "idGame", "nameGame"])

    idUsers_list = rates_df["idUser"].tolist()

    return idUsers_list


def rates_without_reced(rates_df, id):
    new_rates_df = rates_df[rates_df["idUser"] != id]

    return new_rates_df


def pearson_correlation_df(users_list, user_rate):
    pearson_dic = {}

    for name, group in users_list:
        group = group.sort_values(by="idGame")
        # print(group)
        user_rate = user_rate.sort_values(by="idGame")

        nRates = len(group)
        temp_df = user_rate[user_rate["idGame"].isin(
            group["idGame"].tolist())]
        temp_list = temp_df["score"].tolist()
        temp_group_list = group["score"].tolist()
        # print("------")
        # print(temp_list)
        # print("------")
        # print(temp_group_list)

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


def game_recommendation(pearson_df, rates_df, id):
    topUsers = pearson_df.sort_values(by="simIndex", ascending=False)[0:50]
    topUsersRated = topUsers.merge(
        rates_df, left_on="idUser", right_on="idUser", how="inner")

    topUsersRated["weighRated"] = topUsersRated["simIndex"] * \
        topUsersRated["score"]

    tempSumRatings = topUsersRated.groupby(
        "nameGame").sum()[["simIndex", "weighRated"]]

    # print(tempSumRatings)
    tempSumRatings.columns = ["sumSimIndex", "sumWeighRated"]
    # print(topUsersRated)

    recommendation_df = pd.DataFrame()

    recommendation_df["weight avg rec score"] = tempSumRatings["sumWeighRated"] / \
        tempSumRatings["sumSimIndex"]

    recommendation_df["nameGame"] = tempSumRatings.index

    recommendation_df = recommendation_df.sort_values(
        by="weight avg rec score", ascending=False)

    return recommendation_df


def filter_games_played(recommendation_df, user_list):
    return recommendation_df[~recommendation_df["nameGame"].isin(
        user_list["nameGame"].tolist())]


def post_recommendation(recommendation_df_filtered, id):
    try:
        driver = pymongo.MongoClient(os.environ.get("DB_CONNECT"))
        db = driver[os.environ.get("DB")]
        col = db[os.environ.get("COL2")]
        for name in recommendation_df_filtered["nameGame"].head(3).drop(columns=["weight avg rec score"]):
            game = {
                "idUser": id,
                "nameGame": name
            }
            col.insert_one(game)
        print("Recomendations successfully inserted!!!")
    except:
        print("Could not connect to MongoDB")


if __name__ == "__main__":
    load_envs()
    rates_df = get_rates()
    idUsers_list = get_idUsers(rates_df)

    for id in idUsers_list:
        print("\niteration id: " + id + "\n")
        user_rec_list = get_user_list(rates_df, id)
        new_rates_df = rates_without_reced(rates_df, id)
        users_list = groupByUsers_sorted(rates_df, user_rec_list, new_rates_df)
        pearson_df = pearson_correlation_df(
            users_list, user_rec_list)

        recommendation_df = game_recommendation(pearson_df, new_rates_df, id)
        recommendation_df_filtered = filter_games_played(
            recommendation_df, user_rec_list)
        post_recommendation(recommendation_df_filtered, id)
        # print("------")
        # print(recommendation_df_filtered.head(10))
        # print("------")
        # print(user_rec_list.head(10))
