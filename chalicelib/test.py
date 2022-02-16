import logging
import os.path
from datetime import datetime

import pandas as pd
import datetime
import json

import chalicelib.agg as agg
from chalicelib import metrics as metrics_lib

import vbdb.db as db

from chalicelib import util

import re
import chalicelib

from chalicelib import dbcalls
import math
import boto3
from chalicelib.dbcalls import (
    get_engine,
)

import traceback
import calendar





    
def get_yoy_change(period):
    period_df = pd.read_sql(f"Select * from weather_metric_table where period ='{period}';",con=util.get_db_conn_string())
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


def earning_date_to_quarter_start(date,fiscal_yr_end_month,debug=False):
    # this happens on month + 1. just subtract 1 month
    in_month = (int(date.strftime('%m')) - fiscal_yr_end_month  -1)% 12
    # in quarter 0-4
    in_quarter = math.floor((in_month ) / 3)
    
    target_quarter = (in_quarter -1)  % 4
    target_month = (target_quarter+1) * 3
    if debug:
        print(in_month,in_quarter,target_quarter,target_month)
    target_year = date.year if target_month < in_month else date.year-1
    # print(in_month,in_quarter,target_quarter)
    month = (target_month + fiscal_yr_end_month - 1) % 12 + 1
    return datetime.date(target_year,month,calendar.monthrange(target_year, month)[1])
def get_revenue_df(ticker,debug=False):
    try:
        revenue_df = pd.read_sql(f"""Select * from revenue_table where "Page_URL" like '%%{ticker}%%';""", util.get_db_conn_string())
        revenue_df['Ticker'] = revenue_df.Page_URL.apply(lambda x: re.search("(q=)(.*)", x).group(2))
        revenue_df = revenue_df[~revenue_df.Ticker.isna()]
        revenue_df.Ticker = revenue_df.Ticker.apply(lambda x: x.replace("(", "").replace(")", ""))
        revenue_df = revenue_df.drop('Unnamed: 0', axis=1)
        if debug:
            return revenue_df
    except:
        print("problem getting revenue df for ",ticker)
        raise RuntimeError(ticker)

    try:
        big_list = []
        i = 0
        for x in revenue_df[revenue_df.Ticker == ticker].iloc[0][9:-1].values:
            if i % 8 == 0:
                big_list.append([])
            big_list[-1].append(x)
            i += 1
        revenues = pd.DataFrame(big_list, columns=list(map(lambda x: x.replace("1", ""), revenue_df.iloc[0][9:17].index))).drop("Guide", axis=1)
    
        revenues.Date = revenues.Date.apply(util.to_date)
        revenues.ticker = ticker
        revenues = revenues.sort_values("Date")
        for c in revenues.columns[2:]:
            revenues[c] = revenues[c].apply(util.dollar_to_float)
        revenues = revenues.sort_values("Date")
        revenues['EXP_GROWTH'] = revenues.Cons_Rev / revenues.shift(1).Rev
        revenues['BEAT'] = revenues.Rev > revenues.Cons_Rev
        revenues['GROWTH'] = revenues.Rev / revenues.shift(1).Rev
    except:
        print("problems with processing revenues for ",ticker)
        raise RuntimeError(ticker)
    return revenues    

def get_yoy_weather_for_backtesting(ticker):
    signal_df = pd.read_sql(f"""Select * from weather_yoy_metric_table where ticker='{ticker}' and 
                period = 'quarter' and region ='Total';""",con=util.get_db_conn_string())
    signal_df = signal_df.sort_values("date",ascending=False).drop_duplicates(["date"])
    
    metrics = ['cold_count', 'heat_count',
           'rain_and_sleet', 'rain_count', 'sleet', 'sleet_count', 'snow',
           'snow_count', 'temp_24h_mean', 'weekend_cold_count',
           'weekend_heat_count', 'weekend_rain_count', 'weekend_sleet_count',
           'weekend_snow_count']
    signal_df.drop(["ticker","region","update_day","period"],axis=1)
    values_df = signal_df[metrics+["date"]].iloc[[0]]
    input_df = signal_df[metrics+["date"]].copy()
    
    return values_df,input_df
def get_signed_yoy_weather_for_backtesting(ticker):
    signal_df = pd.read_sql(f"""Select * from weather_signed_yoy_metrics where ticker='{ticker}';""",con=util.get_db_conn_string())
    signal_df = signal_df.sort_values("date",ascending=False).drop_duplicates(["date"])
    
    metrics = ['cold_count', 'heat_count',
           'rain_and_sleet', 'rain_count', 'sleet', 'sleet_count', 'snow',
           'snow_count', 'temp_24h_mean', 'weekend_cold_count',
           'weekend_heat_count', 'weekend_rain_count', 'weekend_sleet_count',
           'weekend_snow_count']
    values_df = signal_df[metrics+["date"]].iloc[[0]]
    input_df = signal_df[metrics+["date"]].copy()
    
    return values_df,input_df
def save_backtesting_on_ticker(ticker,date_string,debug=False,test=False):
    try:
        fiscal_year_end =dbcalls.get_db_conn().session.query(db.Company).filter(db.Company.id ==dbcalls.get_ticker_id()[ticker]).all()[0].fiscal_yr_end

        metrics = ['cold_count', 'heat_count',
           'rain_and_sleet', 'rain_count', 'snow',
           'snow_count', 'temp_24h_mean', 'weekend_cold_count',
           'weekend_heat_count', 'weekend_rain_count',
           'weekend_snow_count']
        values_df,input_df = get_signed_yoy_weather_for_backtesting(ticker)
        assert len(input_df) > 0, "input df is empty"
        def get_sign_for_metric(metric,date):
            return 1
        for m in metrics:
            input_df[m] = input_df[m] * input_df.date.apply(lambda date: get_sign_for_metric(m,date)) > 0
    
    
        revenue_ticker_df = get_revenue_df(ticker)[["Date","Qtr","BEAT"]]
        revenue_ticker_df["quarter_end"]=revenue_ticker_df.Date.apply(lambda x: pd.to_datetime(
            earning_date_to_quarter_start(x,fiscal_year_end.month)))
        assert len(revenue_ticker_df) > 0 , "revenue df empty"
        scored_df = pd.merge(revenue_ticker_df,input_df,left_on="quarter_end",right_on="date").sort_values("date").drop(["Date","quarter_end"],axis=1)
        assert len(scored_df) > 0, "scored df empty"
        signal, prediction, strength,meta_df  = metrics_lib.new_get_best_signal(scored_df.iloc[1:],scored_df.iloc[[0]])
        assert len(prediction) > 0 and len(signal) >0 ,"prediction is empty"
        results_dict = {}
    
        results_dict["final_value"] = prediction.values[0]
        results_dict["final_wr"] = strength.values[0]
        for k,v in meta_df.items():
            results_dict[k]=v
        results_dict["update_date"] = date_string
        results_dict["ticker"] = ticker
        results_dict["final_wr"]=strength[0]
        # results_dict["final_value"]=   prediction[0]
        
        wr_df = scored_df.copy()
        for c in wr_df.columns:
            if c!="BEAT":
                wr_df[c+"_wr"] = wr_df[c]==wr_df.BEAT
                
        results_dict.update(wr_df[[c for c in wr_df.columns if c!="BEAT" and c!="Qtr" and c!="date"]].iloc[1:].mean().to_dict())
        results_dict.update(values_df)
        assert len(values_df) > 0
        if debug:
            return results_dict 
        else:
            if not test:
                pd.DataFrame(results_dict, index=[0]).to_sql("saved_weather_backtesting", con=dbcalls.get_db_conn_string(), index=False, if_exists="append")
                delete_query = """ DELETE FROM saved_weather_backtesting where update_date not in (select max(update_date) from saved_weather_backtesting) and ticker = '{ticker}';"""
                with dbcalls.get_engine().connect() as con:
                    con.execute(delete_query)
            if test:
                return pd.DataFrame(results_dict, index=[0])
    except Exception as e:
        print("failed on ",ticker,date_string, e)
        print(traceback.format_exc())
def save_backtesting(event):
    date_string =  str(datetime.date.today())
    for record in event:
        body = json.loads(record.body)
        for item in body:
            ticker = item["ticker"]
            save_backtesting_on_ticker(ticker,date_string)
    return {"outcome":"success"}




def write_weather_yoy(period,debug=False):
    df = get_yoy_change(period)
    date_string =  str(datetime.date.today())
    df["update_day"] = date_string
    df["period"] = period
    if debug:
        return df
    else:
        df.to_sql("weather_yoy_metric_table",util.get_db_conn_string(),if_exists='append',index=False)
        try:    
            delete_query = """ 
            DELETE FROM weather_yoy_metric_table wm
            USING (
                    SELECT *,row_number() OVER (PARTITION BY period,ticker,region,date ORDER BY update_day DESC)  as rn 
                    FROM weather_yoy_metric_table
            ) del
            WHERE del.period = wm.period
            AND del.region = wm.region
            AND del.date = wm.date
            AND del.update_day = wm.update_day
            and del.ticker = wm.ticker
            AND del.rn >1
            ;"""
            with get_engine().connect() as con:
                con.execute(delete_query)
        except Exception as e:
            print(e)
            print(period,"deletion")