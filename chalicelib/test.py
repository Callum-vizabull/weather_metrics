import logging
import os.path
from datetime import datetime

import pandas as pd
import datetime
import json

import chalicelib.agg as agg
import chalicelib.datafun as datafun
from chalicelib.datafun import load_weather_daily_dataframe, generate_quarterly_excel,generate_monthly_excel

import vbdb.db as db

from chalicelib import util

conn_string = 'postgres+psycopg2://postgres:vizavizavizaviza@postgres-prod-2020-05-11-06-52-cluster.cluster-ciuvybput2xn.us-east-1.rds.amazonaws.com/vizabase'
_DB_CONN = db.DBConnection(url=conn_string)

companies = list(_DB_CONN.session.query(db.Company))

def test(period,lookback=180):
    start_date = datetime.date.today() - datetime.timedelta(days=lookback)
    df = load_weather_daily_dataframe(start_date,connection = conn_string)

    df_list = []
    for i in range(4):
        companies_in_qtr = [c for c in companies if c.weather and len(c.locations)>0 and c.fiscal_yr_end_month % 4 == i
                           and sum([x.store_count for x in c.locations]) > 0]

        date= datetime.date(2021,i+1,1)
        if period=="week":
            period_string="custom_weekly"
        if period == "month":
            period_string = "MS"
        if period =="quarter":
            period_string = "QS-" + date.strftime('%b').upper()
        df_list.append(datafun.generate_excel(df,period=period_string,companies=companies_in_qtr))
    large_df_list = []
    for i in range(4):
        temp_df_list = []
        for j in range(len(df_list[i])):
            if "eights" in df_list[i][j][0]:
                continue
            df_temp = df_list[i][j][1].reset_index().melt(id_vars=["Ticker","index"])
            df_temp.columns = ["ticker","region","date","value"]
            df_temp["column_name"] = df_list[i][j][0]
            temp_df_list.append(df_temp)
        large_df_list.append(pd.concat(temp_df_list,axis=0).pivot_table(values="value",columns="column_name",index=["ticker","region","date"]).reset_index())
    test_df = pd.concat(large_df_list,axis=0)
    oldest_date = test_df.date.min()
    return test_df[test_df.date!=oldest_date]
    
def get_yoy_change(period):
    period_df = pd.read_sql(f"Select * from weather_metric_table where period ='{period}';",con=conn_string)
    metrics = [c for c in period_df.columns if c not in ["ticker","region","date","update_day","period"]]
    if period =="quarter" or period=="month":
        period_df["period_of_year"] = period_df.date.apply(lambda x : (x.month))
    else:
        period_df["period_of_year"] = period_df.date.apply(lambda x :util.get_week(x)[1])
    period_df["last_year"] = period_df.date.apply(lambda x: x.year -1)
    period_df["this_year"] = period_df.date.apply(lambda x: x.year)
    combined_df = period_df.merge(period_df,left_on=["region","ticker","period_of_year","last_year"],right_on=["region","ticker","period_of_year","this_year"],suffixes=("_new","_old"))
    for c in metrics:
        combined_df[c] = combined_df[c+"_new"]- combined_df[c+"_old"]
    combined_df["date"] = combined_df.date_new
    combined_df = combined_df[["region","ticker","date"]+metrics].copy()
    melt_df = combined_df.melt(id_vars=["region","ticker","date"])
    melt_df["heatmap"] = melt_df.groupby(["region","ticker","variable"]).value.transform(
                         lambda x: pd.qcut(x, 7,labels=False,duplicates="drop"))
    melt_df.variable = melt_df.variable +"_heatmap"
    melt_df["heatmap"] -= 3
    pivot_df = melt_df.pivot_table(index=["region","ticker","date"],columns="variable",values="heatmap")
    pivot_df = pivot_df.reset_index()
    final_df = combined_df.merge(pivot_df,on=["region","ticker","date"])
    return final_df
def metrics_qtd_weather(ticker,start,end):
    query = f"Select * from weather_yoy_metric_table where  ticker='{ticker}' and date < '{str(end)}' and date > '{str(start)}';"
    df_result = pd.read_sql(query, con=util.get_db_conn_string())
    return json.dumps(list(df_result.to_dict('records')), default=util.alchemyencoder)
