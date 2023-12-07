# %%
# Data Science Related
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype
import streamlit as st

# Scraping Related
import re
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from pyvirtualdisplay import Display

# SQL Related
from sqlalchemy import create_engine, URL, text

# Notification Related
from twilio.rest import Client

# General
import datetime as dt
import time
import pytz
from dotenv import dotenv_values
import os


# SETUP FUNCTIONS
# %%
def import_secrets(type):
    if type == ".env":
        secrets = dotenv_values("C:/Users/Brayden/Desktop/Pow_Hounder/.env")
    if type == "streamlit":
        secrets = st.secrets
    return secrets


# %%
def create_sql_engine(secrets):
    try:
        sql_url_obj = URL.create(
            "mysql+mysqlconnector",
            username="admin",
            password=secrets["AWS_RDS_SQL_PW"],
            host=secrets["AWS_RDS_SQL_HOSTNAME"],
            port="3306",
            database="Pow_Hounder",
        )
        engine = create_engine(sql_url_obj, pool_size=20, max_overflow=10)
        print("Connected to SQL server")
    except:
        print("Connection Error")
    return engine


# %%
def twilio_setup(secrets):
    client = Client(secrets["TWILIO_SID"], secrets["TWILIO_TOKEN"])
    return client


# %%
def create_selenium_driver():
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    firefox_options.set_preference("browser.download.folderList", 2)
    firefox_options.set_preference("browser.download.manager.showWhenStarting", False)
    firefox_options.set_preference("browser.download.dir", os.getcwd())
    firefox_options.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")

    driver = webdriver.Firefox(
        options=firefox_options,
    )
    driver.implicitly_wait(2)

    return driver


# %%
def deploy_sql_engine_streamlit():
    secrets = import_secrets("streamlit")
    engine = create_sql_engine(secrets)
    return engine


# %%
def deploy_drivers_and_engines():
    secrets = import_secrets(".env")
    engine = create_sql_engine(secrets)
    client = twilio_setup(secrets)
    return engine, client


# SCRAPE AND SQL PUSH FUNCTIONS
# %%
def dl_lift_status(retries=3):
    """ """
    driver = create_selenium_driver()
    driver.get("https://www.mammothmountain.com/on-the-mountain/mountain-report-winter")
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "Lift_inner__I42dG"))
        )
        time.sleep(3) # wait for javascript to load
        lift_elem_list = driver.find_elements(By.CLASS_NAME, "Lift_inner__I42dG")
        
    except TimeoutException:
        lift_elem_list = []

    lift_names = []
    lift_statuses = []
    lift_update_times = []

    for entry in lift_elem_list:
        try:
            lift_name = entry.text.splitlines()[0] # First line
            if lift_name is None:
                lift_name = ""
        except:
            lift_name = ""

        try:
            lift_status = entry.text.splitlines()[2] # Third line
            if lift_status is None:
                lift_status = ""
        except:
            lift_status = ""

        try:
            lift_update_time = re.findall(
                r"\b((1[0-2]|0?[1-9]):([0-5][0-9]) ([AaPp][Mm]))", entry.text
            )[2][0]
            lift_update_time = dt.datetime.strptime(
                lift_update_time, "%H:%M %p"
            ).strftime("%H:%M:%S")
            if lift_update_time is None:
                lift_update_time = ""
        except:
            lift_update_time = ""

        lift_names.append(lift_name)
        lift_statuses.append(lift_status)
        lift_update_times.append(lift_update_time)

    data_scrape_times = [
        dt.datetime.now()
        .astimezone(pytz.timezone("US/Pacific"))
        .strftime("%Y-%m-%d %H:%M:%S")
    ] * len(lift_names)

    d = pd.DataFrame(
        {
            "lift_name": lift_names,
            "lift_status": lift_statuses,
            "lift_update_time": lift_update_times,
            "data_scrape_time": data_scrape_times,
        }
    )
    driver.close()
    return d


# %%
def dl_wind_dat():
    driver = create_selenium_driver()
    wind_dat_page_link = "https://mammothmountain.westernweathergroup.com/"
    wind_dat_dl_butt = """//*[@id="Body"]/div/div/div[2]/div/div/div/div[1]/div/a[2]"""
    driver.get(wind_dat_page_link)
    val = driver.find_element("xpath", wind_dat_dl_butt)
    wind_dat_csv_link = val.get_attribute("href")
    df = pd.read_csv(wind_dat_csv_link)
    df["data_scrape_time"] = (
        dt.datetime.now()
        .astimezone(pytz.timezone("US/Pacific"))
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    df["Date"] = df["Date"].apply(
        lambda x: dt.datetime.strptime(x, "%m/%d/%y").strftime("%Y:%m:%d")
    )
    cols = [
        "station",
        "date_of_reading",
        "time_of_reading",
        "wind_spd",
        "wind_dir",
        "wind_gust",
        "temp",
        "wind_chill",
        "rel_humidity",
        "dew_pt",
        "wet_bulb",
        "daily_max_wind",
        "time_of_max_wind",
        "max_wind_dir",
        "daily_min_temp",
        "daily_max_temp",
        "yesterday_max_gust",
        "yesterday_min_temp",
        "yesterday_max_temp",
        "data_scrape_time",
    ]
    df = df.set_axis(cols, axis=1)
    driver.close()
    return df


# %%
def dl_snow_dat():
    driver = create_selenium_driver()
    snow_dat_page_link = (
        "https://www.onthesnow.com/california/mammoth-mountain-ski-area/skireport"
    )
    driver.get(snow_dat_page_link)
    time.sleep(5)
        
    try:
        # Todays snowfall
        snow_elements = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'styles_snow__Djsgc'))
    )
        snow_elem = snow_elements[7]
    except TimeoutException:
        snow_elem = None

    try:
        snowfall = snow_elem.text
        snowfall = int(re.search(r"\d+", snowfall).group(0))
    except:
        snowfall = None

    data_scrape_time = dt.datetime.now().astimezone(pytz.timezone("US/Pacific"))
    data_scrape_time = data_scrape_time.strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame(
        data={"snowfall": [snowfall], "data_scrape_time": [data_scrape_time]}
    )
    driver.close()
    return df
dl_snow_dat()

# %%
def push_snow_dat(snow_dat, engine):
    snow_dat.to_sql("Snow_Log", engine, if_exists="append", index=False)
    print(f"df pushed {dt.datetime.now()}")


def push_wind_dat(wind_dat, engine):
    wind_dat.to_sql("Wind_Log", engine, if_exists="append", index=False)
    print(f"df pushed {dt.datetime.now()}")


def push_lift_dat(lift_dat, engine):
    lift_dat.to_sql("Lift_Status_Log", engine, if_exists="append", index=False)
    print(f"df pushed {dt.datetime.now()}")


# %%
def is_now_in_time_period(start_time, end_time, now_time):
    if start_time < end_time:
        return now_time >= start_time and now_time <= end_time
    else:
        # Over midnight:
        return now_time >= start_time or now_time <= end_time


# %%
def perform_lift_scrape(
    poll_int=300, start_time=dt.time(5, 30), end_time=dt.time(17, 30)
):
    engine, twilio_client = deploy_drivers_and_engines()
    while True:
        while is_now_in_time_period(
            start_time,
            end_time,
            now_time=dt.datetime.now().astimezone(pytz.timezone("US/Pacific")).time(),
        ):
            lift_dat = dl_lift_status()
            print(lift_dat)
            push_lift_dat(lift_dat, engine)
            time.sleep(poll_int)


def perform_wind_scrape(poll_int=900):
    engine, twilio_client = deploy_drivers_and_engines()
    while True:
        wind_dat = dl_wind_dat()
        push_wind_dat(wind_dat, engine)
        time.sleep(poll_int)


def perform_snow_scrape(poll_int=3600):
    engine, twilio_client = deploy_drivers_and_engines()
    while True:
        snow_dat = dl_snow_dat()
        push_snow_dat(snow_dat, engine)
        time.sleep(poll_int)


# NOTIFICATION RELATED FUNCTIONS
# %%
def check_for_lift_status_change(df_before, df_now):
    try:
        df_differences = df_before["lift_status"].compare(df_now["lift_status"])
    except Exception as e:
        df_differences = (
            pd.DataFrame()
        )  # If the comparison doesn't work for whatever reason, it will output an empty dataframe
        print(e)

    if not df_differences.empty:
        update_str_list = []
        for ind in df_differences.index:
            updated_lift_name = df_before.loc[ind, "lift_name"]
            updated_lift_prev_status = df_differences.loc[ind, "self"]
            updated_lift_new_status = df_differences.loc[ind, "other"]
            update_str = f"{updated_lift_name} \n{updated_lift_prev_status} -> {updated_lift_new_status}"
            update_str_list.append(update_str)
            update_full_str = " \n \n".join(update_str_list)
            update_full_str = "Pow-Hounder Update: \n \n" + update_full_str
    else:
        update_full_str = None
    return update_full_str


# %%
def check_valid_phone_number(engine):
    with engine.connect() as conn:
        df = pd.read_sql(text("Active_Notify_Numbers"), conn)
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    todays_date = pd.to_datetime("today").normalize()
    phone_list_series = df[
        (todays_date >= df["start_date"]) & (todays_date <= df["end_date"])
    ]["phone_number"]
    phone_list = phone_list_series.to_list()
    return phone_list


# %%
def lift_status_notifier(int=300):
    engine, twilio_client = deploy_drivers_and_engines()
    df_before = dl_lift_status()  # Initialize for initial comparison

    while True:
        while is_now_in_time_period(
            start_time=dt.time(5, 30),
            end_time=dt.time(17, 30),
            now_time=dt.datetime.now().astimezone(pytz.timezone("US/Pacific")).time(),
        ):  # only run in a timeframe that where we expect lift status to change or be relevant
            while phone_number_list := check_valid_phone_number(
                engine
            ):  # only run the scraper when there is a number requesting we do so
                phone_number_list = list(
                    set(phone_number_list)
                )  # remove duplicates from list
                df_now = dl_lift_status()
                lift_update_str = check_for_lift_status_change(df_before, df_now)
                df_before = df_now  # reset comparison
                print(df_before)
                print(dt.datetime.now())
                if lift_update_str:
                    print(lift_update_str)
                    message = twilio_client.messages.create(
                        body=lift_update_str, from_="+18336914492", to=phone_number_list
                    )
                time.sleep(int)


# %%
def push_number(phone_number, user_notif_date_range):
    engine = deploy_sql_engine_streamlit()

    phone_number = f"+1{phone_number}"  # add +1
    start_date = user_notif_date_range[0].strftime("%Y-%m-%d")
    end_date = user_notif_date_range[1].strftime("%Y-%m-%d")
    df = pd.DataFrame(
        {
            "phone_number": [phone_number],
            "start_date": [start_date],
            "end_date": [end_date],
        }
    )
    df.to_sql("Active_Notify_Numbers", engine, if_exists="append", index=False)


def rem_number(phone_number):
    engine = deploy_sql_engine_streamlit()
    phone_number = f"+1{phone_number}"  # add +1
    with engine.connect() as conn:
        statement = text(
            f"""DELETE FROM Active_Notify_Numbers WHERE phone_number='{phone_number}'"""
        )
        conn.execute(statement)
        conn.commit()


def pull_scrape_sample():
    engine = deploy_sql_engine_streamlit()
    with engine.connect() as conn:
        # lift_status log
        statement = text(
            f"""SELECT * FROM Lift_Status_Log order by liftstatusID desc LIMIT 25"""
        )
        df_lift_status_log = pd.read_sql_query(
            statement,
            conn,
            index_col="liftstatusID",
            parse_dates=["data_scrape_time"],
        )
        df_lift_status_log = df_lift_status_log.astype(str)

        # wind log
        statement = text(f"""SELECT * FROM Wind_Log order by windlogID desc LIMIT 23""")
        df_wind_log = pd.read_sql_query(
            statement,
            conn,
            index_col="windlogID",
        )
        df_wind_log = df_wind_log.astype(str)

        # snow log
        statement = text(f"""SELECT * FROM Snow_Log order by snowlogID desc LIMIT 10""")
        df_snow_log = pd.read_sql_query(
            statement,
            conn,
            index_col="snowlogID",
        )
        df_snow_log = df_snow_log.astype(str)

    return df_lift_status_log, df_wind_log, df_snow_log


# %%
