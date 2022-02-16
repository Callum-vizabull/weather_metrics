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


def generate_monthly_excel(df: pd.DataFrame, period: str, companies: [Company],
                           metadata: dict = {}):
        # excel = pd.ExcelWriter(output, engine="openpyxl")
    names, df_list = zip(*generate_excel(df, period, companies))
    df_list, names = list(df_list), list(names)
    df_list.append(pd.DataFrame.from_dict(metadata, orient='index'))
    names = ['Mean', 'Precip', 'Precip Days', 'Precip Weekend Days', 'Snow',
                 'Snow Days', 'Snow Weekend Days', 'Sleet', 'Sleet Days', 'Sleet Weekend Days',
                 'Cold', 'Weekend Cold', 'Heat', 'Weekend Heat', 'Region Weights',
                 'State Weights', 'Metadata']
    return df_list, names

def generate_quarterly_excel(df: pd.DataFrame, quarter_end: datetime, companies: [Company],
                             metadata: dict = {}):
    q_freq = "Q-" + quarter_end.strftime('%b').upper()
    # grouped = df.groupby(pd.Grouper(key="date", freq=q_freq))
    # good_dfs = []
    # for t in grouped.groups:
    #     if t.month == quarter_end.month:
    #         good_dfs.append(grouped.get_group(t))
    # good_df = pd.concat(good_dfs)

    # _, monthly_dfs = zip(*generate_excel(good_df, "M", [company], metadata))
    _, quarterly_dfs = zip(*generate_excel(good_df, q_freq, companies, metadata))
    # monthly_dfs, quarterly_dfs = list(monthly_dfs), list(quarterly_dfs)
    quarterly_dfs =  list(quarterly_dfs)

    quarterly_dfs.append(pd.DataFrame.from_dict(metadata, orient='index'))
    # monthly_dfs.append(pd.DataFrame.from_dict(metadata, orient='index'))

    # drop non quarter dates
    quarterly_dfs = list(map(lambda x: x.drop("Ticker", axis=1).rename(lambda x: "Quarter ending in " +
                                                                                 str(pd.to_datetime(
                                                                                     x).month) + "-" + str(
        pd.to_datetime(x).year), axis=1), quarterly_dfs[:-3])) + quarterly_dfs[-3:]
    names = ['Mean', 'Precip', 'Precip Days', 'Precip Weekend Days', 'Snow',
             'Snow Days', 'Snow Weekend Days', 'Sleet', 'Sleet Days', 'Sleet Weekend Days',
             'Cold', 'Weekend Cold', 'Heat', 'Weekend Heat', 'Region Weights',
             'State Weights', 'Metadata']
    # return monthly_dfs,quarterly_dfs,names
    return quarterly_dfs,names




def region_and_state_weights(locations):
    df = pd.DataFrame([{k: v for k, v in vars(l).items() if not k.startswith('_')} for l in locations])
    state_weights = pd.DataFrame(data={})
    region_weights = pd.DataFrame(data={})
    for region in agg.default_us_region_groupings.items():
        states = region[1]
        states.sort()
        region = region[0]
        region_df = df[df['iso_region'].isin(states)]
        state_weights = state_weights.append(pd.DataFrame(
            data={'iso-region': states, 'store_count': region_df['store_count'] / region_df['store_count'].sum()}))
        region_weights = region_weights.append(pd.DataFrame(
            data={'region': [region], 'store_count': [region_df['store_count'].sum() / df['store_count'].sum()]}))
    state_weights = state_weights.set_index('iso-region').sort_index(ascending=True)
    region_weights = region_weights.set_index('region').sort_index(ascending=True)
    return region_weights, state_weights


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
        df["year"] = df.date.apply(lambda x : x.year)
        state_df = df.groupby([df.airport_code.map(agg.airports_to_states), "date"]).agg(week_location_agg
                                                                                         ).reset_index().groupby(
            ["airport_code", "week_of_year"]).agg(week_time_agg).reset_index().drop("week_of_year",axis=1)
            
    else:
        state_df = df.groupby([df.airport_code.map(agg.airports_to_states), "date"]).agg(location_agg
                                                                                         ).reset_index().groupby(
            ["airport_code", pd.Grouper(key="date", freq=period)]).agg(time_agg).reset_index()
    # merges number of airports per state as a column to each entry. Used to normalize
    state_df = state_df.merge(
        pd.DataFrame.from_dict({k: [len(v)] for k, v in agg.states_to_airports.items()}, orient='index',
                               columns=["aiport_count"]), left_on="airport_code", right_index=True)

    # Linear transforms on data.
    state_df.temp_24h_mean = state_df.temp_24h_mean.apply(c_to_f)
    state_df.rain_and_sleet = state_df.rain_and_sleet.apply(mm_to_inch)
    state_df.rain_count = state_df.rain_count.divide(state_df['aiport_count'], axis=0)
    state_df.weekend_rain_count = state_df.weekend_rain_count.divide(state_df['aiport_count'], axis=0)
    state_df.snow = state_df.snow.apply(mm_to_inch)
    state_df.snow_count = state_df.snow_count.divide(state_df['aiport_count'], axis=0)
    state_df.weekend_snow_count = state_df.weekend_snow_count.divide(state_df['aiport_count'], axis=0)
    state_df.sleet = state_df.sleet.apply(mm_to_inch)
    state_df.sleet_count = state_df.sleet_count.divide(state_df['aiport_count'], axis=0)
    state_df.weekend_sleet_count = state_df.weekend_sleet_count.divide(state_df['aiport_count'], axis=0)
    state_df.cold_count = state_df.cold_count.divide(state_df['aiport_count'], axis=0)
    state_df.weekend_cold_count = state_df.weekend_cold_count.divide(state_df['aiport_count'], axis=0)
    state_df.heat_count = state_df.heat_count.divide(state_df['aiport_count'], axis=0)
    state_df.weekend_heat_count = state_df.weekend_heat_count.divide(state_df['aiport_count'], axis=0)

    df_list = aggregate(state_df, companies, list(location_agg.keys()))

    all_states = pd.DataFrame(data={})
    all_regions = pd.DataFrame(data={})
    for company in companies:
        region_weights, state_weights = region_and_state_weights(company.locations)
        region_weights.insert(0, column="Ticker", value=f'{company.ticker}')
        state_weights.insert(0, column="Ticker", value=f'{company.ticker}')
        all_states = all_states.append(state_weights)
        all_regions = all_regions.append(region_weights)

    df_list.append(("Region Weights", all_regions))
    df_list.append(("State Weights", all_states))

    return df_list


def aggregate(df1: pd.DataFrame, companies: list, value_names: list):
    state_df = pd.DataFrame(df1)

    company_metric_dict = {}
    for company in companies:
        company_metric_dict[company] = {}
        region_weights, state_weights = region_and_state_weights(company.locations)

        logger.debug(f'Analysing: {company.ticker}')

        # apply state weights
        state_weighted_df = state_df.merge(state_weights['store_count'], left_on="airport_code", right_on="iso-region")
        state_weighted_df[value_names] = state_weighted_df[value_names].mul(state_weighted_df.store_count, axis=0)

        # groupby region
        region_group_df = state_weighted_df.groupby(["date", state_weighted_df.airport_code.map(
            agg.states_to_regions)]).sum().reset_index()

        # apply region weights to get nationwide results
        total_weight_df = region_group_df.merge(region_weights['store_count'], left_on="airport_code",
                                                right_on="region", suffixes=("", "_region"))
        total_weight_df[value_names] = total_weight_df[value_names].mul(total_weight_df.store_count_region, axis=0)
        total_weight_df = total_weight_df.groupby(["airport_code", "date"], as_index=False).sum()

        # add total row
        for v in value_names:
            output_df = pd.DataFrame(
                region_group_df.pivot(index="date", columns="airport_code", values=v).sort_index(ascending=False).T)
            output_df = output_df.append(pd.DataFrame(
                total_weight_df.pivot(index="date", columns="airport_code", values=v).sort_index(ascending=False).T.sum(), columns=["Total"]).T)
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