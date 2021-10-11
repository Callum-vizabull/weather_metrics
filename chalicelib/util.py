import base64
import decimal
import json
import logging
import math
import datetime
import pandas as pd
import boto3
import decimal
import os
from dateutil.relativedelta import relativedelta

import vbdb.db as db


from botocore.exceptions import ClientError



_DB_CONN = None
_DB_CONN_STRING = None
_DB_CONN_PARAM_NAME = '/darksky/db-conn'

_S3_CLIENT = None
_SSM_CLIENT = None
_SQS_CLIENT = None
_DYNAMODB_RESOURCE = None

logger = logging.getLogger(__name__)


# def get_dynamodb_resource():
#     global _DYNAMODB_RESOURCE
#     if _DYNAMODB_RESOURCE is None:
#         _DYNAMODB_RESOURCE = boto3.resource('dynamodb')
#     return _DYNAMODB_RESOURCE


def get_sqs_client():
    global _SQS_CLIENT
    if _SQS_CLIENT is None:
        _SQS_CLIENT = boto3.client('sqs')
    return _SQS_CLIENT


def get_ssm_client():
    global _SSM_CLIENT
    if _SSM_CLIENT is None:
        _SSM_CLIENT = boto3.client('ssm')
    return _SSM_CLIENT


# def get_api_key():
#     global _DARKSKY_API_KEY
#     if _DARKSKY_API_KEY is None:
#         base64_key = get_ssm_client().get_parameter(Name=_API_KEY_PARAM_NAME, WithDecryption=True)['Parameter']['Value']
#         _DARKSKY_API_KEY = base64.b64decode(base64_key).decode('utf-8')
#     return _DARKSKY_API_KEY


def get_db_conn_string():
    global _DB_CONN_STRING
    if _DB_CONN_STRING is None:
        base64_key = get_ssm_client().get_parameter(Name=_DB_CONN_PARAM_NAME, WithDecryption=True)['Parameter']['Value']
        _DB_CONN_STRING = base64.b64decode(base64_key).decode('utf-8')
    return _DB_CONN_STRING


def get_db_conn():
    global _DB_CONN
    if _DB_CONN is None:
        _DB_CONN = db.DBConnection(url=get_db_conn_string())
    return _DB_CONN


def get_s3_client():
    global _S3_CLIENT
    if _S3_CLIENT is None:
        _S3_CLIENT = boto3.client('s3')
    return _S3_CLIENT


def get_sqs_url(queue_name):
    sqs_client = get_sqs_client()
    sqs_queue_url = sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
    return sqs_queue_url

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)
def send_sqs_message(sqs_queue_url, msg_body):
    """
    :param sqs_queue_url: String URL of existing SQS queue
    :param msg_body: String message body
    :return: Dictionary containing information about the sent message. If
        error, returns None.
    """
    # Send the SQS message
    sqs_client = get_sqs_client()
    try:
        msg = sqs_client.send_message(QueueUrl=sqs_queue_url,
                                      MessageBody=json.dumps(msg_body, cls=DecimalEncoder))
    except ClientError as e:
        logger.error(e)
        raise e
    return msg

def get_end_month_from_ticker(ticker):
    return get_db_conn().session.query(db.Company).filter(db.Company.ticker == ticker).first().fiscal_yr_end_month
# class DecimalEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, decimal.Decimal):
#             return str(o)
#         return super(DecimalEncoder, self).default(o)

def get_week(date):
    mod = math.floor(day_of_year(date)/7)
    mod = min(mod,51)
    return mod
def day_of_year(date):
    first = datetime.date(date.year,1,1)
    return (date - first).days 
def get_week_date(date):
    week = get_week(date)
    if week == 51:
        return datetime.datetime(date.year+1,1,1)-datetime.timedelta(1) 
    delta = (week+1) * 7 -1
    first = datetime.datetime(date.year,1,1)
    new = first+ datetime.timedelta(delta)
    if new.year == first.year:
        return new
    else:
        return datetime.datetime(new.year,1,1)-datetime.timedelta(1) 
def get_month(date):
    return (date.month,date.year)
def get_month_date(date):
    return date+  pd.offsets.MonthEnd(1)
    
    
#####################################


def to_date(x):
    l= x.split('/')
    l[2] = "20"+l[2]
    l[0],l[1]=l[1],l[0]
    l = [int(x) for x in l]
    return datetime.date(*l[::-1])
def dollar_to_float(s):
    if s is None:
        return 0 
    
    if "B" in s:
        return float(s.replace("$","").replace("B",""))*1e9 
    if "M" in s:
        return float(s.replace("$","").replace("M",""))*1e6 
    return float(s.replace("$",""))
def round_date(date):
    if date.day > 15:
        return date+  pd.offsets.MonthBegin(1)
    return datetime.date(date.year,date.month,1)
        

def generate_time_df(end):
    dates = []
    start = datetime.date(2010,12,1)
    start= start +  pd.offsets.MonthBegin(1) - datetime.timedelta(1)
    dates = [start + relativedelta(months=+3*x)for x in range (50)]
    qtr = ["Q"+str((i+3)%4+1) + str(d.year)[2:] for i,d in enumerate(dates)]
    return pd.DataFrame(zip(dates,qtr),columns=["Date","Qtr"])



def combine_sites(df1,signals):
    df = df1.copy()
    df["reach_views"] = df.reachpermillion * df.pageviewsperuser
    df=df[signals+["Qtr","reach_views","date"]].groupby(["Qtr","date"],as_index=False).sum()
    # df = df.drop("pageviewsperuser",axis=1)
    df['pageviewsperuser'] = df["reach_views"]/df["reachpermillion"]
    df = df.drop("reach_views",axis=1)

    return df
def combine_sites_for_company(df1,signals):
    df = df1.copy()
    df["reach_views"] = df.reachpermillion * df.pageviewsperuser
    df=df[signals+["Qtr","reach_views"]].groupby("Qtr",as_index=False).sum()
    df['pageviewsperuser'] = df["reach_views"]/df["reachpermillion"]
    df = df.drop("reach_views",axis=1)

    return df
_TICKER_ID = None
_ID_WEBID = None
def get_ticker_id():
    global _TICKER_ID
    if _TICKER_ID is None:
        _TICKER_ID = {c.ticker: c.id for c in get_db_conn().session.query(db.Company).all()}
    return _TICKER_ID
def get_id_webid():
    global _ID_WEBID
    if _ID_WEBID is None:
        _ID_WEBID = {c.company_id: c.id for c in get_db_conn().session.query(db.Website).all()}
    return _ID_WEBID

_QUEUES= None
_ID_TICKER = None
_SITES = None
_SIGNAL_QUEUE = None
def get_signal_queue():
    global _SIGNAL_QUEUE
    if _SIGNAL_QUEUE is None:
        _SIGNAL_QUEUE = get_sqs_url(os.getenv('SIGNAL_QUEUE'))
    return _SIGNAL_QUEUE
def get_queues():
    global _QUEUES
    if _QUEUES is None:
        _QUEUES = (get_sqs_url(os.getenv('DOMAIN_QUEUE')), get_sqs_url(os.getenv('TICKER_QUEUE')), get_sqs_url(os.getenv('REVENUE_QUEUE')))
    return _QUEUES
def get_id_ticker():
    global _ID_TICKER
    if _ID_TICKER is None:
        _ID_TICKER ={c.id: c.ticker for c in get_db_conn().session.query(db.Company).all()}
    return _ID_TICKER
def get_sites():
    global _SITES
    if _SITES is None:
        _SITES = list(get_db_conn().session.query(db.Company).order_by(db.Website.id).filter(db.Website.alexa_api_run).all())
    return _SITES
    
    
def alchemyencoder(obj):
    """JSON encoder function for SQLAlchemy special classes."""
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, db.VizaBase):
        dictret = dict(obj.__dict__)
        dictret.pop('_sa_instance_state', None)
        return dictret
    