# -*- coding: utf-8 -*-
"""
Author:Ying-Jung Chen
Task:
This py script includes the function to query daily weather api historical data(2001-2009)
by month from weather AP and save these file into AWS S3 bucket. The file named "v1_scout_trial_latlon_for_twc_query.csv" 
including "trial_id","lat","lon" columns. The start and stop year, as well as the start 
and stop date can be changed based on the query period.
"""

## import the requeired library
import os
from pathlib import Path
from datetime import datetime, timedelta

import boto3
import numpy as np
import pandas as pd
import requests
import PingCredentials

## Access weather API credentials

AUTH_POINT = 'https:/URL/as/token.oauth2'
session = requests.Session()
CLIENT_ID = "My-project"
session.auth = PingCredentials(
    client_id=CLIENT_ID,
    client_secret=os.environ[CLIENT_ID.replace("-", "_")],
    ping_token_endpoint=AUTH_POINT,
)

## Download data from AWS S3 bucket
client = boto3.client("s3")
filename="my_file.csv"
local_filepath = "/mnt/"+filename

# This is from my own AWS S3 bucket
S3_bucket = "xx.xx.xx.xxx"
S3_key = "xx/xxxx/xxxxxx/xxxxxxx/"+filename

client.download_file(
    Filename=local_filepath, Bucket=S3_bucket, Key=S3_key,
)

## Select daily (UTC) raw weather data and you can add new feature here

feature_to_query_array = [
    "date",
    "max_temperature",
    "total_precipitation",
    "avg_wind_speed",
    "avg_relative_humidity",
]

## Read data
p = Path("/mnt")
df_trial_latlon = pd.read_csv(p /+filename)

def daily_weather_query_by_month(resp_json):
    """
    This daily_weather_query_by_month function to query daily weather data for 
    the selected months in the selected time periods (years)
    
    Input Parameters
    ----------
    resp_json: json format 
               This is recieve json from weather API  
    Returns
    -------
    feature_data_month_df: pandas dataframe
               This is the results of query daily weather data by month
       
    """
    # The length of dates in the query from historical data
    dt_n = len(resp_json["historical"][0]["data"])
    # An initial empty list to retrieve the results
    feature_data_month_agg = []

    for i in range(dt_n):
        data_dic_query_dt = resp_json["historical"][0]["data"][i]
        feature_data_dt = [data_dic_query_dt[x] for x in feature_to_query_array]
        feature_data_month_agg.append(feature_data_dt)

        # Convert the list of retrieve features to dataframe
        feature_data_month_df = pd.DataFrame(
            feature_data_month_agg, columns=feature_to_query_array
        )
        # Add data source in the dataframe (necessary step)
        feature_data_month_df["datasource"] = resp_json["historical"][0]["gridsInfo"][0]["dataSource"]

    return feature_data_month_df

def historical_one_loc_query(
    feature_to_query_array,
    loc_lat,
    loc_lon,
    start_year,
    stop_year,
    start_date="01-01",
    stop_date="12-31",
):

    """
    Query the historicl weather data for the weather variables and interested 
    time period via API approach. For example, create an array for selected
    feature and provide the coordinates for location of interests as well as 
    the time period (start, stop year)

    Input Parameters
    ----------
    feature_to_query_array : np.array
                            the selected features
    loc_lat: np.array
             latitude for selected field location 
    loc_lon: np.array
             longitute for selected field location
    start_year: string
                   start year "yyyy" of interested time period
    stop_year: string
                   stop year "yyyy" of interested time period
    start_date: string
                   start month and date "mm-dd" of start year
                   default "01-01"
    stop_date: string
                   stop month and date "mm-dd" of stop year
                   default "12-31"
    Returns
    -------
    feature_query_result_df: pandas dataframe
        Returns the dataframe with many columns for the selected features 
        and the interest of time period and location 
    """

    # An inital empthy dataframe with selected feature columns
    result_col = feature_to_query_array.copy()
    result_col.append("datasource")
    feature_query_result_df = pd.DataFrame(columns=result_col)

    # Provide selected start and stop year for url (can change year and date for your interest)
    start_string = start_year + "-" + start_date
    stop_string = stop_year + "-" + stop_date

    # Convert start and stop year string to datetime object
    # Provide the current date retrieved by the json url, which is initialized with start string
    to_currentdate = datetime.strptime(start_string, "%Y-%m-%d")
    target = datetime.strptime(stop_string, "%Y-%m-%d")

    feature_query_result_df = pd.DataFrame()

    while to_currentdate < target:
        # Convert the to_currentdate back to string format to retrieve the json url
        start_string = to_currentdate.strftime("%Y-%m-%d")
        resp = session.get(
            "https://URL/historical/daily",
            params={
                "geocode": str(loc_lat) + "," + str(loc_lon),
                "units": "m",
                "fromDate": start_string,
                "toDate": stop_string,
                "dataSource": "TWC",
            },
        )

        resp_json = resp.json()

        # This "to_currentdate" update the current date from json response
        to_currentdate = datetime.strptime(resp_json["metadata"]["toDate"], "%Y-%m-%d")
        df = feature_query_result_df.copy()
        feature_query_result_df = df.append(daily_weather_query_by_month(resp_json))
        # Add one day to avoid the repeatative of last day within the month
        to_currentdate = to_currentdate + timedelta(days=1)
    return feature_query_result_df

def data_query_save2_S3(
    local_path, S3_path, Bucket_name, trialid_latlon_df, start_year, stop_year, start_date="01-01", stop_date="12-31",):
    """
    This data_query_save2_S3 function to save query daily weather data for locations
    in selected months during the selected time periods (years) within 
    local directory and upload these files to S3 bucket 
    
    Input Parameters
    ----------
    local_path: string format
                   This is file path on local directory  
    S3_path: string format 
                   This is folder/file path on S3 bucket
    Bucket_name: string format
                   This is S3 bucket_name
    trialid_latlon_df: pandas dataframe
                   This is a dataframe with id, 
                   latitude, and longitude 
    start_year: string
                   start year "yyyy" of interested time period
    stop_year: string
                   stop year "yyyy" of interested time period
    start_date: string
                   start month and date "mm-dd" of start year
                   default "01-01"
    stop_date: string
                   stop month and date "mm-dd" of stop year
                   default "12-31"
    
    """
    # Error handling array
    error_index_array = []

    # Query daily_twc_data based on selected features,location, time period
    for i in range(trialid_latlon_df.shape[0]):
        try:
            query_one_loc_df = historical_one_loc_query(
                feature_to_query_array,
                trialid_latlon_df.iloc[i]["latitude"],
                trialid_latlon_df.iloc[i]["longitude"],
                start_year,
                stop_year,
                start_date,
                stop_date,
            )
            trial_id = trialid_latlon_df.iloc[i]["trial_id"]
            ## save the query dataframe as csv in local directory and upload to S3 bucket
            file_name = (
                "weather" + start_year + "_" + stop_year + "_daily_" + trial_id + ".csv"
            )
            local_file_storage = local_path + file_name
            query_one_loc_df.to_csv(local_file_storage)
            S3_storage = S3_path + file_name
            client.upload_file(
                Filename=local_file_storage,
                Bucket=Bucket_name,
                Key=S3_storage,
            )
        except Exception as e:
            print(str(i + 1) + " with error " + str(e))
            error_index_array.append(i)

## Use the data_query_save2_S3 function to query weather data
## based dataframe with trial_id, lat, and lon information.
## Then,save the query files into S3 bucket.

# Define file paths
local_path = "/mnt/query_results/"
S3_path = "xx/xxxxx/xxxxxxx/xxxxxxx/"
Bucket_name = "xxx.xxxxx.xxxxxx.xxxxxx"

# Use the save_to_S3 function (this case is from 2001-2009)
data_query_save2_S3(local_path, S3_path, Bucket_name, df_trial_latlon, "2001", "2009", "01-01", "12-31")