# -*- coding: utf-8 -*-

"""
Vizabull data-analysis functions

"""
import logging
import os.path
from datetime import datetime

import pandas as pd
import sqlalchemy as sa

import chalicelib.agg as agg
import vbdb.db as db

from vbdb.db import Company
logger = logging.getLogger(__name__)

from chalicelib import util
import requests
import numpy as np

def load_weather_daily_dataframe(start_date: datetime,
                                 end_date: datetime = datetime.now(),
                                 connection: str = os.getenv("DB_URL"),
                                 iso_regions=None,
                                 optimize=False):
    """
    load raw weather_daily based on start and end dates, filtered by iso_region
    :param start_date: start date
    :param end_date: end date
    :param connection: DB connection string to use, default is env(DB_URL)
    :param iso_regions: default None. If specified only date for the region(s) will be returned
    :param optimize: default False. Whether to reduce the number of columns returned for the data to a minimal set
    :return: dataframe containing weather_daily data
    """

    select = f"""SELECT 
            wd.weather_station, 
            ws.name,
            ws.airport_code, 
            lo.name, 
            lo.iso_region, 
            wd.date, 
            wd.latitude, 
            wd.longitude, 
            wd.temp_max,
            wd.temp_mean,
            wd.temp_24h_mean,
            wd.temp_min, 
            wd.temp_high, 
            wd.temp_low, 
            wd.cloud_cover, 
            wd.dew_point, 
            wd.humidity, 
            wd.precip_intensity, 
            wd.precip_intensity_max, 
            wd.precip_type,
            wd.pressure, 
            wd.uv_index, 
            wd.visibility, 
            wd.wind_gust, 
            wd.wind_speed
        FROM 
            weather_daily AS wd 
        JOIN 
            weather_station as ws 
        ON 
            wd.weather_station = ws.id 
        JOIN location as lo 
        ON 
            ws.iso_region = lo.iso_region
        WHERE
            wd.date between :start AND :end
    """

    if optimize:
        select = f"""SELECT 
                ws.airport_code, 
                lo.iso_region, 
                wd.date, 
                wd.temp_max,
                wd.temp_24h_mean,
                wd.precip_intensity, 
                wd.precip_type
            FROM 
                weather_daily AS wd 
            JOIN 
                weather_station as ws 
            ON 
                wd.weather_station = ws.id 
            JOIN location as lo 
            ON 
                ws.iso_region = lo.iso_region
            WHERE
                wd.date between :start AND :end
        """

    if iso_regions:
        select += f" AND lo.iso_region in :values"

    query = sa.text(select)
    if iso_regions:
        query = query.bindparams(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'),
                                 values=tuple(iso_regions))
    else:
        query = query.bindparams(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))

    # read the SQL query from the database
    query_df = pd.read_sql(query, con=connection)

    # enrich the data frame by adding extra columns per precipitation type
    # TODO: this should be mapped by the scraper
    precip_intensity = list(query_df["precip_intensity"])
    precip_type = list(query_df["precip_type"])
    rain = []
    sleet = []
    snow = []
    # loop through the precip_type column
    for i in range(len(precip_type)):
        if precip_type[i] == "rain":
            rain.append(precip_intensity[i])
            sleet.append(0)
            snow.append(0)
        elif precip_type[i] == "sleet":
            rain.append(0)
            sleet.append(precip_intensity[i])
            snow.append(0)
        elif precip_type[i] == "snow":
            rain.append(0)
            sleet.append(0)
            snow.append(precip_intensity[i])
        else:
            rain.append(0)
            sleet.append(0)
            snow.append(0)
    # add the extra column to the dataframe, multiplying up to daily values (but keeping mm)
    query_df["hourly_rain"] = rain
    query_df["hourly_sleet"] = sleet
    query_df["hourly_snow"] = snow
    # convert hourly values to daily to match temps
    query_df["rain"] = h_to_d(query_df["hourly_rain"])
    query_df["sleet"] = h_to_d(query_df["hourly_sleet"])
    query_df["snow"] = h_to_d(query_df["hourly_snow"])

    return query_df


def c_to_f(data):
    return (data * 9 / 5) + 32


def f_to_c(data):
    return (data - 32) * 9 / 5


def mm_to_inch(data):
    return 0.0393701 * data


def inch_to_mm(data):
    return 25.4 * data


def h_to_d(data):
    return data * 24


def get_companies_by_month(month=datetime.today().month - 1):
    if month is None:
        month = datetime.today().month - 1
    path = __file__.split("vizabull")[0]
    print(path)
    cal = pd.read_csv(path + "/Earnings Calendar.csv")
    return list(cal[(cal['Q1'] % 3) == (month % 3)].Ticker.values)




def generate_excel(df1: pd.DataFrame, period: str, companies: [Company], metadata: dict = {}):
    df = pd.DataFrame(df1)

    df['rain_and_sleet'] = df['rain'] + df['sleet']  # NB.
    # df['rain'] = df['rain_and_sleet']
    precip_limit = lambda ts: (mm_to_inch(ts) > 0.04)
    heat_limit = lambda ts: (c_to_f(ts) > 95)
    cold_limit = lambda ts: (c_to_f(ts) < 20)

    value_columns = ['temp_max', 'temp_24h_mean', 'rain', 'sleet', 'snow', 'rain_and_sleet', "store_count"]
    # These columns describe the operation applied to aggegrate each column
    location_agg = {
        "temp_24h_mean": "mean",
        "rain_and_sleet": "mean",
        "rain_count": "sum",
        "weekend_rain_count": "sum",
        "snow": "mean",
        "snow_count": "sum",
        "weekend_snow_count": "sum",
        "sleet": "mean",
        "sleet_count": "sum",
        "weekend_sleet_count": "sum",
        "cold_count": "sum",
        "weekend_cold_count": "sum",
        "heat_count": "sum",
        "weekend_heat_count": "sum",
    }
    time_agg = {
        "temp_24h_mean": "mean",
        "rain_and_sleet": "sum",
        "rain_count": "sum",
        "weekend_rain_count": "sum",
        "snow": "sum",
        "snow_count": "sum",
        "weekend_snow_count": "sum",
        "sleet": "sum",
        "sleet_count": "sum",
        "weekend_sleet_count": "sum",
        "cold_count": "sum",
        "weekend_cold_count": "sum",
        "heat_count": "sum",
        "weekend_heat_count": "sum",
    }
    week_time_agg = dict(time_agg)
    week_time_agg["date"] = "first"
    week_location_agg = dict(location_agg)
    week_location_agg["week_of_year"] = "first"
    
    mask = df.date.apply(lambda x: x.weekday() > 4)

    df['rain_count'] = df.rain_and_sleet.apply(lambda x: int(precip_limit(x)))
    df['weekend_rain_count'] = df.rain_count
    df.loc[~mask, 'weekend_rain_count'] = 0
    df['snow_count'] = df.snow.apply(lambda x: int(precip_limit(x)))
    df['weekend_snow_count'] = df.snow_count
    df.loc[~mask, 'weekend_snow_count'] = 0
    df['sleet_count'] = df.sleet.apply(lambda x: int(precip_limit(x)))
    df['weekend_sleet_count'] = df.sleet_count
    df.loc[~mask, 'weekend_sleet_count'] = 0

    df['cold_count'] = df.temp_max.apply(lambda x: int(cold_limit(x)))
    df['weekend_cold_count'] = df.cold_count
    df.loc[~mask, 'weekend_cold_count'] = 0

    df['heat_count'] = df.temp_max.apply(lambda x: int(heat_limit(x)))
    df['weekend_heat_count'] = df.heat_count
    df.loc[~mask, 'weekend_heat_count'] = 0

    # Aggregates the data by state and then by 
    if period == "custom_weekly":

        df["week_of_year"] = df.date.apply(util.get_week)
        # df["year"] = df.date.apply(lambda x : x.year)
        state_df = df.groupby(
            ["airport_code", "week_of_year"]).agg(week_time_agg).reset_index().drop("week_of_year",axis=1)
            
    else:
        state_df = df.groupby(
            ["airport_code", pd.Grouper(key="date", freq=period)]).agg(time_agg).reset_index()

    df_list = aggregate(state_df, companies, list(location_agg.keys()))
    return df_list

def attach_store_counts(df,ticker):
    df = df.copy()
    store_count_request_json = requests.get(f'https://5y8t2c1iwb.execute-api.us-east-1.amazonaws.com/api/places/{ticker}').json()
    airport_to_store_count = {d["airport_code"]:d["count"] for d in store_count_request_json[0]["store_counts"]}
    df["store_count"]  = df.airport_code.map(airport_to_store_count).fillna(0.0)
    return df


def aggregate(df1: pd.DataFrame, companies: list, value_names: list):
    state_df = pd.DataFrame(df1)

    company_metric_dict = {}
    for company in companies:
        company_metric_dict[company] = {}
        # region_weights, state_weights = region_and_state_weights(company.locations)

        logger.debug(f'Analysing: {company.ticker}')
        
        # add in weights
        # create total and regional dfs

        state_df = attach_store_counts(state_df,company.ticker)
        state_df = state_df.fillna(0.0)
        weighted_mean = lambda x: 0 if state_df.loc[x.index, "store_count"].sum() == 0 or pd.isna(state_df.loc[x.index, "store_count"].sum()) else np.average(x, weights=state_df.loc[x.index, "store_count"])

 

        total_group_df = state_df.groupby(["date"]).agg(weighted_mean).reset_index()
        total_group_df["region"] = "Total"
        state_df["region"] = state_df.airport_code.map(agg.airport_to_region)
        state_df = state_df.drop("airport_code",axis=1)

        # test_group = state_df[~state_df.region.isna()].groupby(["date", "region"])
        
        # for c in state_df.columns:
        #     if c == "date" or c=="region":
        #         continue
        #     print(c)
        #     print(test_group[[c]].agg(weighted_mean))
        region_group_df = state_df[~state_df.region.isna()].groupby(["date", "region"]).agg(weighted_mean).reset_index()

        for v in value_names:
            output_df = pd.DataFrame(
                region_group_df.pivot(index="date", columns="region", values=v).sort_index(ascending=False).T)
            output_df = output_df.append(
                total_group_df.pivot(index="date", columns="region", values=v).sort_index(ascending=False).T)
            # add ticker column
            output_df.insert(0, column="Ticker", value=f'{company.ticker}')
            company_metric_dict[company][v] = output_df

    results = []
    for v in value_names:
        temp = pd.DataFrame()
        for company in companies:
            temp = temp.append(company_metric_dict[company][v])
        results.append((v, temp))
    return results
