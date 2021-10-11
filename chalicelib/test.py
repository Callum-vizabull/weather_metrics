import logging
import os.path
from datetime import datetime

import pandas as pd
import datetime

import chalicelib.agg as agg
import chalicelib.datafun as datafun
from chalicelib.datafun import load_weather_daily_dataframe, generate_quarterly_excel,generate_monthly_excel

import vbdb.db as db

conn_string = 'postgres+psycopg2://postgres:vizavizavizaviza@postgres-prod-2020-05-11-06-52-cluster.cluster-ciuvybput2xn.us-east-1.rds.amazonaws.com/vizabase'
_DB_CONN = db.DBConnection(url=conn_string)

companies = list(_DB_CONN.session.query(db.Company))

def test(period):
    start_date = datetime.date.today() - datetime.timedelta(days=180)
    df = load_weather_daily_dataframe(start_date,connection = conn_string)
    
    # return  datafun.generate_monthly_excel(df,period="Q-JAN",companies=[ c for c in companies if c.ticker in ["WMT","HOG"]])
    # return  datafun.generate_excel(df,period="custom_weekly",companies=[ c for c in companies if c.ticker in ["WMT","HOG"]])

    df_list = []
    for i in range(4):
        companies_in_qtr = [c for c in companies if c.weather and len(c.locations)>0 and c.fiscal_yr_end_month % 4 == i
                           and sum([x.store_count for x in c.locations]) > 0]
        print(len(companies_in_qtr))
        print(set([c.fiscal_yr_end_month for c in companies_in_qtr]))
        date= datetime.date(2021,i+1,1)
        if period=="week":
            period_string="weekly_custom"
        if period == "month":
            period_string = "MS"
        if period =="quarter":
            period_string = "Q-" + date.strftime('%b').upper()
        df_list.append(datafun.generate_excel(df,period="custom_weekly",companies=companies_in_qtr))
    
    print(len(df_list[0]))
    large_df_list = []
    for i in range(4):
        temp_df_list = []
        for j in range(len(df_list[i])-3):
            print(j)
            df_temp = df_list[i][j][1].reset_index().melt(id_vars=["Ticker","index"])
            df_temp.columns = ["ticker","region","date","value"]
            df_temp["column_name"] = df_list[i][j][0]
            temp_df_list.append(df_temp)
            print(df_list[i][j][0])
        print([df.columns for df in temp_df_list])
        large_df_list.append(pd.concat(temp_df_list,axis=0))
    test_df = large_df_list[0].pivot_table(values="value",columns="column_name",index=["ticker","region","date"]).reset_index()
    return test_df
   