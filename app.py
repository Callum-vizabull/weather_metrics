from chalice import Chalice

import datetime
app = Chalice(app_name='weather_metrics')

from chalicelib.test import test 
import chalicelib.test as create_dataframes
@app.route('/')
def index():
    return {'hello': 'world'}
 
import vbdb.db as db

conn_string = 'postgres+psycopg2://postgres:vizavizavizaviza@postgres-prod-2020-05-11-06-52-cluster.cluster-ciuvybput2xn.us-east-1.rds.amazonaws.com/vizabase'
_DB_CONN = db.DBConnection(url=conn_string)

from chalicelib.dbcalls import (
    get_engine,
)

def write_weather_by_period(period,lookback=180):
    df = test(period,lookback)
    date_string =  str(datetime.date.today())
    df["update_day"] = date_string
    df["period"] = period
    df.to_sql("weather_metric_table",conn_string,if_exists='append',index=False)
    try:    
        delete_query = """
        DELETE FROM weather_metric_table
            WHERE update_day NOT IN
            (
                SELECT MAX(update_day) AS max_date
                FROM weather_metric_table
                GROUP BY ticker,region,period,date
            );"""
        with get_engine().connect() as con:
            con.execute(delete_query)
    except Exception as e:
        print(e)
        print(period,"deletion")

def write_weather_yoy(period):
    df = create_dataframes.get_yoy_change(period)
    date_string =  str(datetime.date.today())
    df["update_day"] = date_string
    df["period"] = period
    return df
    df.to_sql("weather_yoy_metric_table",conn_string,if_exists='append',index=False)
    try:    
        delete_query = """
        DELETE FROM weather_yoy_metric_table
            WHERE update_day NOT IN
            (
                SELECT MAX(update_day) AS max_date
                FROM weather_yoy_metric_table
                GROUP BY ticker,region,period,date
            );"""
        with get_engine().connect() as con:
            con.execute(delete_query)
    except Exception as e:
        print(e)
        print(period,"deletion")
        
schedule_string ='cron(03 00 ? * * *)'
@app.schedule(schedule_string)
def update_weather_week(event):
        return write_weather_by_period("week")
@app.schedule(schedule_string)
def update_weather_month(event):
        return write_weather_by_period("month")
@app.schedule(schedule_string)
def update_weather_year(event):
        return write_weather_by_period("quarter")
yoy_schedule_string ='cron(24 01 ? * * *)'
@app.schedule(yoy_schedule_string)
def update_weather_yoy(event):
    write_weather_yoy("quarter")
    write_weather_yoy("month")
    return write_weather_yoy("week")
    
@app.route('/weather_qtd/{ticker}/{start_date}/{end_date}')
def weather_qtd(ticker,start_date,end_date):
    return create_dataframes.metrics_qtd_weather(ticker,start_date,end_date)

