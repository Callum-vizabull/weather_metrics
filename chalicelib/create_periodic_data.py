import vbdb.db as db

import chalicelib.util as util
import chalicelib.dbcalls as dbcalls
import chalicelib.new_datafun as new_datafun
import chalicelib.old_datafun as old_datafun

import datetime
import pandas as pd
import json
import requests

companies = list(util.get_db_conn().session.query(db.Company))
def has_new_store_counts(ticker):
    r = requests.get(f'https://5y8t2c1iwb.execute-api.us-east-1.amazonaws.com/api/places/{ticker}')
    x = r.json()
    return len(x) > 0 

def consolidate_weather_by_period(ticker,period,lookback=180):
    return test(ticker,period,lookback=180)
def test(ticker,period,lookback=180):
    start_date = datetime.date.today() - datetime.timedelta(days=lookback)
    use_new = has_new_store_counts(ticker)
    
    if use_new:
        df = new_datafun.load_weather_daily_dataframe(start_date,connection = util.get_db_conn_string())
    else:
        df = old_datafun.load_weather_daily_dataframe(start_date,connection = util.get_db_conn_string())

    df_list = []
    # companies 
    for i in range(3):
        companies_in_qtr = [c for c in companies if c.weather and len(c.locations)>0 and c.fiscal_yr_end_month % 3 == i
                           and sum([x.store_count for x in c.locations ]) > 0 and c.ticker == ticker]
        if len(companies_in_qtr) < 1:
            continue
        date= datetime.date(2021,i+1,1)
        if period=="week":
            period_string="custom_weekly"
        if period == "month":
            period_string = "MS"
        if period =="quarter":
            period_string = "QS-" + date.strftime('%b').upper()
        if use_new:
            quarter_df = new_datafun.generate_excel(df,period=period_string,companies=companies_in_qtr)
        else:
            quarter_df = old_datafun.generate_excel(df,period=period_string,companies=companies_in_qtr)
        assert len(quarter_df) > 0, f"quarter_df {i} is empty"
        df_list.append(quarter_df)
    large_df_list = []
    for i in range(len(df_list)):
        temp_df_list = []
        for j in range(len(df_list[i])):
            if "eights" in df_list[i][j][0].lower():
                continue
            if use_new:
                df_temp = df_list[i][j][1].reset_index().melt(id_vars=["Ticker","region"])
            else:
                df_temp = df_list[i][j][1].reset_index().melt(id_vars=["Ticker","index"])

            df_temp.columns = ["ticker","region","date","value"]
            df_temp["column_name"] = df_list[i][j][0]
            temp_df_list.append(df_temp)
        large_df_list.append(pd.concat(temp_df_list,axis=0).pivot_table(values="value",columns="column_name",index=["ticker","region","date"]).reset_index())
    test_df = pd.concat(large_df_list,axis=0)
    return test_df[test_df.date> pd.to_datetime(start_date)]

def write_weather_by_period(ticker,period,lookback=180):
    df = consolidate_weather_by_period(ticker,period,lookback)
    date_string =  str(datetime.date.today())
    df["update_day"] = date_string
    df["period"] = period
    df.to_sql("weather_metric_table",util.get_db_conn_string(),if_exists='append',index=False)
    try:    
        delete_query = """ 
        DELETE FROM weather_metric_table wm
        USING (
                SELECT *,row_number() OVER (PARTITION BY period,date,ticker,region ORDER BY update_day DESC)  as rn 
                FROM weather_metric_table
        ) del
        WHERE del.period = wm.period
        AND del.region = wm.region
        AND del.date = wm.date
        AND del.update_day = wm.update_day
        and del.ticker = wm.ticker
        AND del.rn >1
        ;"""
        with dbcalls.get_engine().connect() as con:
            con.execute(delete_query)
    except Exception as e:
        print(e)
        print(period,"deletion")
        
def write_periodic_date(event):
    for record in event:
        body = json.loads(record.body)
        for item in body:
            ticker = item["ticker"]
            write_weather_by_period(ticker,"quarter")
            write_weather_by_period(ticker,"month")
            write_weather_by_period(ticker,"week")

    return {"outcome":"success"}