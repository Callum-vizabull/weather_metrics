#!/usr/bin/python
# -*- coding: utf-8 -*-
import datetime
import re

import sqlalchemy as sql_db
import vbdb.db as db
from chalicelib.util import (get_db_conn_string,get_db_conn,
to_date,
dollar_to_float)



def get_revenue_df(ticker, revenue_df):
    import pandas as pd
    big_list = []
    i = 0
    for x in revenue_df[revenue_df.Ticker == ticker].iloc[0][9:-1].values:
        if i % 8 == 0:
            big_list.append([])
        big_list[-1].append(x)
        i += 1
    revenues = pd.DataFrame(big_list, columns=list(map(lambda x: x.replace("1", ""), revenue_df.iloc[1][9:17].index))).drop("Guide", axis=1)

    # revenues = pd.DataFrame(np.reshape(revenue_df[revenue_df.Ticker == ticker].iloc[0][9:-1].values, (-1,8)),columns=list(map(lambda x: x.replace("1",""),revenue_df.iloc[1][9:17].index))).drop("Guide",axis=1)
    revenues.Date = revenues.Date.apply(to_date)
    revenues.ticker = ticker
    revenues = revenues.sort_values("Date")
    # revenues = revenues[~revenues.Rev.isna()]
    # revenues = revenues[~revenues.isna().any(axis =1)]
    for c in revenues.columns[2:]:
        revenues[c] = revenues[c].apply(dollar_to_float)
    revenues = revenues.sort_values("Date")
    revenues['EXP_GROWTH'] = revenues.Cons_Rev / revenues.shift(1).Rev
    revenues['BEAT'] = revenues.Rev > revenues.Cons_Rev
    revenues['GROWTH'] = revenues.Rev / revenues.shift(1).Rev
    return revenues


def get_merged_historical_df(df, ticker):
    import pandas as pd
    revenue_df = pd.read_sql_table("revenue_table", get_db_conn_string())
    revenue_df['Ticker'] = revenue_df.Page_URL.apply(lambda x: re.search("(q=)(.*)", x).group(2))
    revenue_df = revenue_df[~revenue_df.Ticker.isna()]
    revenue_df.Ticker = revenue_df.Ticker.apply(lambda x: x.replace("(", "").replace(")", ""))
    revenue_df = revenue_df.drop('Unnamed: 0', axis=1)
    return pd.merge(get_revenue_df(ticker, revenue_df)[["BEAT", "Qtr"]], df, on="Qtr", how="inner")


def new_signal_from_raw_columns(df, metric):
    
    column_suffix = ["yoyqoq", "accel", "accel_yoy", "accel_break"]
    metric_columns = [metric + "_" + c for c in column_suffix]
    df_scores = df[metric_columns + ["date", "BEAT", "Qtr"]].copy()
    df_scores.loc[:, metric_columns] = df_scores[metric_columns] > 0
    return df_scores.drop("date", axis=1)


def backtest(ticker):

    import pandas as pd
    results_dict = {}
    signals = ["reach", "views_pu", "views_pm"]
    query = "Select * from company_level_metrics where ticker='" + ticker + "' ;"

    alexa_df = pd.read_sql(query, con=get_db_conn_string()).sort_values("index")

    alexa_df["date"] = alexa_df["Date"]
    signal_dfs = [new_signal_from_raw_columns(alexa_df, signal_name) for signal_name in signals]
    historical_signals = [sdf.iloc[:-1] for sdf in signal_dfs]
    current_signals = [pd.DataFrame(sdf.iloc[-1]).T for sdf in signal_dfs]
    metric_signals = [new_get_best_signal(h, c) for h, c in zip(historical_signals, current_signals)]
    i = 0
    
    for h, c, wr,_ in metric_signals:
        results_dict[signals[i] + "_wr"] = wr.values[0]
        results_dict[signals[i] + "_value"] = c.values[0]

        h.columns = [signals[i], "Qtr"]
        i += 1
    combined_metric_historical = pd.merge(metric_signals[0][0],
                                          pd.merge(metric_signals[1][0], metric_signals[2][0], on="Qtr"))
    combined_metric_current = pd.DataFrame(list(zip(*[t[1] for t in metric_signals])), columns=signals)
    combined_metric_historical = pd.merge(combined_metric_historical, historical_signals[0][["Qtr","BEAT"]],on="Qtr" )
    
    signal, prediction, strength,meta_df = new_get_best_signal(combined_metric_historical, combined_metric_current)
    results_dict["final_value"] = prediction.values[0]
    results_dict["final_wr"] = strength.values[0]
    for k,v in meta_df.items():
        results_dict[k]=v
    return results_dict


def new_get_best_signal(df_historical, df_current):
    import pandas as pd
    # print(df_historical)
    df = df_historical[~df_historical.BEAT.isna()].copy()
    truth = df["BEAT"]

    scored_df = pd.DataFrame()
    for c in df_historical.drop(["BEAT", "Qtr"], axis=1):
        scored_df[c] = df[c] == truth
    df_per = pd.DataFrame(scored_df.mean(), columns=["wr"])
    # print(df_per)
    # print(scored_df)
    strong = list(df_per[df_per.wr > .66].index.values)
    weak = list(df_per[~(df_per.wr > .66) & (df_per.wr > .5)].index.values)
    
    # print("cghjgh",df)

    refined_vote_df = pd.DataFrame()
    refined_vote_df["strong_vote"] = df[strong].mean(axis=1) >= .5
    refined_vote_df["raw_vote"] = df[scored_df.columns].mean(axis=1) >= .5
    refined_vote_df["less_negative"] = df[strong + weak].mean(axis=1) >= .5

    refined_prediction = pd.DataFrame()
    refined_prediction["strong_vote"] = df_current[strong].mean(axis=1) >= .5
    refined_prediction["raw_vote"] = df_current.mean(axis=1) >= .5
    refined_prediction["less_negative"] = df_current[strong + weak].mean(axis=1) >= .5
    less_negative = strong + weak
    raw = df_current.columns
    vote_columns = ["strong_vote", "raw_vote", "less_negative"]
    consolidated_scored_df = pd.DataFrame()
    for c in vote_columns:
        consolidated_scored_df[c] = refined_vote_df.loc[:, c] == truth
        refined_vote_df[c+"_score"]= refined_vote_df.loc[:, c] == truth
    wr_df = pd.DataFrame(consolidated_scored_df.mean(), columns=["wr"])
    best_column = wr_df.idxmax().values[0]
    formula = ""
    if best_column =="strong_vote":
        formula= ";".join(strong)
    elif best_column =="less_negative":
        formula = ";".join(less_negative)
    elif best_column=="raw_vote":
        formule = ";".join(raw)
    refined_vote_df["Qtr"] = df["Qtr"]

    num_pos = len(refined_vote_df[refined_vote_df[best_column]])
    num_neg = len(refined_vote_df[~refined_vote_df[best_column]])
    score_pos = (refined_vote_df[refined_vote_df[best_column]][best_column+"_score"]).mean()
    score_neg = (refined_vote_df[~refined_vote_df[best_column]][best_column+"_score"] ).mean()
    recent_correct = refined_vote_df.iloc[0][best_column+"_score"]
    meta_df = {"num_pos":num_pos,"num_neg":num_neg,"score_pos":score_pos,"score_neg":score_neg,"formula":formula,"recent_correct":recent_correct}
    return refined_vote_df[[best_column, "Qtr"]], refined_prediction[best_column], wr_df.T[best_column],meta_df

def date_to_quarter(date):
    q = int((date.month - 1) / 3) + 1
    year = str(date.year)[2:]
    return "Q" + str(q) + year


def signal_from_raw_columns(df, metric):
    column_suffix = ["yoyqoq", "accel", "accel_yoy", "accel_break"]
    metric_columns = [metric + "_" + c for c in column_suffix]
    df_scores = df[metric_columns + ["date"]]
    df_scores = df_scores.loc[~df_scores.isna().any(axis=1)]
    df_scores.loc[:, metric_columns] = df_scores[metric_columns] > 0
    df_scores["Qtr"] = df_scores.date.apply(date_to_quarter)
    return df_scores.drop("date", axis=1)
