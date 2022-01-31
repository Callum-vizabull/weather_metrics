import logging
import os.path
from datetime import datetime

import pandas as pd
import datetime
import json

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

companies = list(util.get_db_conn().session.query(db.Company))


def apply_sign(ticker):
    metrics = ['cold_count', 'heat_count',
       'rain_and_sleet', 'rain_count', 'snow',
       'snow_count', 'temp_24h_mean', 'weekend_cold_count',
       'weekend_heat_count', 'weekend_rain_count',
       'weekend_snow_count']
    query = f"""select * from weather_yoy_metric_table where ticker = '{ticker}' and region='Total' and period='month'; """
    df = pd.read_sql(query,util.get_db_conn_string())
    import datetime
    date= datetime.date(2021,[c for c in companies if c.ticker==ticker][0].fiscal_yr_end_month,1)
    period_string = "Q-" + date.strftime('%b').upper()
    df["month"] = df.date.apply(lambda x: x.month)
    
    rule_df = pd.read_sql(f"""Select * from weather_rule_set where ticker = '{ticker}'""",con=util.get_db_conn_string())
    rule_df = rule_df.drop("ticker",axis=1)
    rule_df = rule_df.apply(pd.to_numeric) # convert all columns of DataFrame

    assert len(rule_df) == 12,f"rule_df not len 12 true len in {len(rule_df)}"
    for c in rule_df.columns:
        if c != "month":
            rule_df = rule_df.rename(columns={c:c+"_rule"})
    merged_df = pd.merge(df,rule_df,on="month")
    assert len(merged_df) > 0, "merged df is empty"
    final_df = merged_df[metrics+["date"]].copy()
    final_df[metrics] = pd.np.multiply(final_df[metrics] ,merged_df[[m+"_rule" for m in metrics]])
    assert len(final_df) > 0, "final_df is empty"

    
    quarter_df = final_df[["date"]+metrics].groupby(pd.Grouper(
        key="date",freq=(period_string))).sum()
    assert len(quarter_df) > 0, "quarter_df is empty"

    quarter_df = quarter_df.reset_index()

    quarter_df["ticker"] = ticker
    delete_query = f"""delete from weather_signed_yoy_metrics where ticker ='{ticker}'"""
    
    with get_engine().connect() as con:
            con.execute(delete_query)
    quarter_df.to_sql("weather_signed_yoy_metrics",index=False,con=util.get_db_conn_string(),if_exists="append")
    assert len(quarter_df) > 0, "quarter_df is empty"

    return quarter_df

def apply_sign_handler(event):
    date_string =  str(datetime.date.today())
    for record in event:
        body = json.loads(record.body)
        for item in body:
            ticker = item["ticker"]
            apply_sign(ticker)
    return {'event': event}

def run_apply_sign():
    sqs_client = boto3.client('sqs')
    sqs_queue_url_ticker = sqs_client.get_queue_url(QueueName="weather-metrics-apply-sign")['QueueUrl']
                                          
    query = "select distinct(wt.ticker) from weather_yoy_metric_table wt inner join weather_rule_set rt on wt.ticker = rt.ticker;"
    tickers = [x[0] for x in pd.read_sql(query,con=util.get_db_conn_string()).values]
    
    for ticker in tickers:
        msg_body = [{"ticker":ticker}]
        sqs_client.send_message(QueueUrl=sqs_queue_url_ticker,
                                              MessageBody=json.dumps(msg_body, cls=util.DecimalEncoder))
    return 1