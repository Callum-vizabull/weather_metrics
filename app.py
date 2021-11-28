from chalice import Chalice

import datetime
app = Chalice(app_name='weather_metrics')

from chalicelib import test 
import chalicelib.test as create_dataframes
@app.route('/')
def index():
    return {'hello': 'world'}
 
import vbdb.db as db



        
schedule_string ='cron(21 01 ? * * *)'
yoy_schedule_string ='cron(40 01 ? * * *)'
backtesting_schedule_string  ='cron(10 02 ? * * *)'
@app.schedule(schedule_string)
def update_weather_week(event):
        return create_dataframes.write_weather_by_period("week")
@app.schedule(schedule_string)
def update_weather_month(event):
        return create_dataframes.write_weather_by_period("month")
@app.schedule(schedule_string)
def update_weather_year(event):
        return create_dataframes.write_weather_by_period("quarter")
@app.schedule(yoy_schedule_string)
def update_weather_yoy(event):
    create_dataframes.write_weather_yoy("quarter")
    create_dataframes.write_weather_yoy("month")
    create_dataframes.write_weather_yoy("week")
    return "Done"
@app.route('/weather_qtd/{ticker}/{start_date}/{end_date}')
def weather_qtd(ticker,start_date,end_date):
    return create_dataframes.metrics_qtd_weather(ticker,start_date,end_date)
import os
@app.on_sqs_message(queue=os.environ['WEATHER_BACKTESTING_QUEUE'], batch_size=1)
def update_backtesting(event):
    return test.save_backtesting(event)
@app.schedule(backtesting_schedule_string)
def update_all_backtesting(event):
    create_dataframes.run_backtesting()