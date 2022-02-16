from chalice import AuthResponse, Chalice

import pandas as pd
import vbdb.db as db

import chalicelib.test as create_dataframes
from chalicelib import signing
import boto3
from chalicelib import util
import json


def queue_periodic_metrics():
    sqs_client = boto3.client('sqs')
    companies = list(util.get_db_conn().session.query(db.Company))
    sqs_queue_url_ticker = sqs_client.get_queue_url(QueueName="build-periodic-metrics-queue")['QueueUrl']
    weather_companies = [c for c in companies if c.weather]
    weather_company_tickers = [c.ticker for c in weather_companies]
    
    for ticker in weather_company_tickers:
        msg_body = [{"ticker":ticker}]
        sqs_client.send_message(QueueUrl=sqs_queue_url_ticker,
                                              MessageBody=json.dumps(msg_body, cls=util.DecimalEncoder))
    return 1
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
    
def run_backtesting():
    sqs_client = boto3.client('sqs')
    sqs_queue_url_ticker = sqs_client.get_queue_url(QueueName="weather-metrics-backtesting")['QueueUrl']
                                          
    query = "Select distinct(ticker) from weather_yoy_metric_table"
    tickers = [x[0] for x in pd.read_sql(query,con=util.get_db_conn_string()).values]
    for ticker in tickers:
        msg_body = [{"ticker":ticker}]
        # print(msg_body)
        sqs_client.send_message(QueueUrl=sqs_queue_url_ticker,
                                              MessageBody=json.dumps(msg_body, cls=util.DecimalEncoder))