from chalice import Chalice

import datetime
app = Chalice(app_name='weather_metrics')

from chalicelib.test import test 
@app.route('/')
def index():
    return {'hello': 'world'}
 
import vbdb.db as db

conn_string = 'postgres+psycopg2://postgres:vizavizavizaviza@postgres-prod-2020-05-11-06-52-cluster.cluster-ciuvybput2xn.us-east-1.rds.amazonaws.com/vizabase'
_DB_CONN = db.DBConnection(url=conn_string)

from chalicelib.dbcalls import (
    get_engine,
)

def write_weather_by_period(period):
    df = test(period)
    date_string =  str(datetime.date.today())
    df["update_day"] = date_string
    df["period"] = period
    df.to_sql("weather_metric_table",conn_string,if_exists='append',index=False)
    try:    
        with get_engine().connect() as con:
            con.execute("DELETE from weather_metric_table where period='"+str(period)+"' and update_day !='"+date_string+"';")
    except Exception as e:
        print(e)
        print(period,"deletion")
schedule_string ='cron(01 59 ? * * *)' 
@app.schedule(schedule_string)
def update_weather_week(event):
        return write_weather_by_period("week")
@app.schedule(schedule_string)
def update_weather_month(event):
        return write_weather_by_period("month")
@app.schedule(schedule_string)
def update_weather_year(event):
        return write_weather_by_period("quarter")
