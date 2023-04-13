# %%
# Data Science Related
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype
import streamlit as st

# Scraping Related
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import lxml
import cchardet
import re
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By

# SQL Related
import mysql.connector as mariadb
from sqlalchemy import create_engine

# Notification Related
from twilio.rest import Client

# General
import datetime as dt
import time
import pytz
from dotenv import dotenv_values
import streamlit as st


# %%
def conn_to_sql(secrets):
    try:
        mariadb_conn = mariadb.connect(
            user="admin",
            password=secrets["AWS_RDS_SQL_PW"],
            host=secrets['AWS_RDS_SQL_HOSTNAME'],
            port="3306",
            database="Pow_Hounder",
        )
        print("Connected to SQL server")
    except mariadb.Error as e:
        if e.errno == mariadb.errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with username or password")
        elif e.errno == mariadb.errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(e)
    return mariadb_conn

# %%
def import_secrets():
    secrets = dotenv_values("C:/Users/Brayden/Desktop/Pow_Hounder/.env")
    return secrets

# %%
def twilio_setup(secrets):
    client = Client(secrets["TWILIO_SID"], secrets["TWILIO_TOKEN"])
    return client


# %%
def selenium_setup():
    service = Service(GeckoDriverManager().install())
    return service


# %%
def create_selenium_driver(service):
    firefoxOptions = Options()
    firefoxOptions.add_argument("--headless")
    driver = webdriver.Firefox(
        options=firefoxOptions,
        service=service,
    )
    driver.implicitly_wait(2)
    return driver


# %%
def dl_lift_status(driver, retries=3):
    """ """
    driver.get("https://www.mammothmountain.com/on-the-mountain/mountain-report-winter")
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "Lifts_inner__okoV3"))
        )
        lift_elem_list = driver.find_elements(By.CLASS_NAME, "Lifts_inner__okoV3")
    except TimeoutException:
        lift_elem_list = []

    lift_names = []
    lift_statuses = []
    lift_update_times = []

    for entry in lift_elem_list:
        try:
            lift_name = re.search(r"^(.+?)(?=\n)", entry.text).group(0)
            if lift_name is None:
                lift_name = ""
        except:
            lift_name = ""

        try:
            lift_status = re.search(r"(?<=\n)(.*?)(?=\n)", entry.text).group(0)
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
                lift_update_time = "aaa"
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
    return d


# %%
def check_for_lift_status_change(df_before, df_now):
    df_differences = df_before["lift_status"].compare(df_now["lift_status"])
    if not df_differences.empty:
        update_str_list = []
        for ind in df_differences.index:
            updated_lift_name = df_before.loc[ind, "lift_name"]
            updated_lift_prev_status = df_differences.loc[ind, "self"]
            updated_lift_new_status = df_differences.loc[ind, "other"]
            update_str = f"{updated_lift_name} \n{updated_lift_prev_status} -> {updated_lift_new_status}"
            update_str_list.append(update_str)
            update_full_str = " \n \n".join(update_str_list)
    else:
        update_full_str = None
    return update_full_str


# %%
def dl_wind_dat(driver):
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
    return df


# %%
def dl_snow_dat(driver):
    snow_dat_page_link = (
        "https://www.onthesnow.com/california/mammoth-mountain-ski-area/skireport"
    )
    driver.get(snow_dat_page_link)
    try:
        # snow_elem = driver.find_element("css selector", "[title^='72 Hour Snowfall']")
        snow_elem = driver.find_element(
            "xpath",
            """//*[@id="__next"]/div[6]/div[2]/div/article[3]/div[2]/div[2]/div[3]/table/tbody/tr[1]/td[8]/span""",
        )
    except TimeoutException:
        snow_elem = None

    try:
        snowfall = snow_elem.text
        snowfall = int(re.search(r"\d+", snowfall).group(0))
    except:
        snowfall = None

    snowfall_scrape_time = data_scrape_times = dt.datetime.now().astimezone(
        pytz.timezone("US/Pacific")
    )
    snowfall_scrape_time = snowfall_scrape_time.strftime("%Y-%m-%d %H:%M:%S")

    return [snowfall, snowfall_scrape_time]


# %%
def push_snow_dat(snow_stat, db_conn):
    cursor = db_conn.cursor()
    cursor.execute(
        f"INSERT INTO Snow_Log (snowfall, data_scrape_time) VALUES ({snow_stat[0]}, '{snow_stat[1]}')"
    )
    db_conn.commit()


# %%
def is_now_in_time_period(start_time, end_time, now_time):
    if start_time < end_time:
        return now_time >= start_time and now_time <= end_time
    else:
        # Over midnight:
        return now_time >= start_time or now_time <= end_time


# %%
def perform_lift_scrape(
    driver, poll_int=300, start_time=dt.time(5, 30), end_time=dt.time(17, 30)
):
    while is_now_in_time_period(
        start_time,
        end_time,
        now_time=dt.datetime.now().astimezone(pytz.timezone("US/Pacific")).time(),
    ):
        lift_dat = dl_lift_status(driver)
        # TODO sql upload code goes here
        time.sleep(poll_int)


# %%
def push_lift_dat(lift_dat, db_conn):
    cursor = db_conn.cursor()
    cursor.execute(
        f'INSERT INTO Snow_Log (lift_name, lift_status, lift_update_time, data_scrape_time) VALUES (\'{lift_dat["lift_name"]}\', \'{lift_dat["lift_status"]}\', \'{lift_dat["lift_update_time"]}\')'
    )
    db_conn.commit()


# %%
def perform_wind_scrape(
    driver, poll_int=900, start_time=dt.time(5, 30), end_time=dt.time(17, 30)
):
    while is_now_in_time_period(
        start_time,
        end_time,
        now_time=dt.datetime.now().astimezone(pytz.timezone("US/Pacific")).time(),
    ):
        wind_dat = dl_wind_dat(driver)
        # TODO sql upload code goes here
        time.sleep(poll_int)


def perform_snow_scrape(driver, poll_int=3600):
    snow_dat = dl_snow_dat(driver)
    # TODO sql upload code goes here
    time.sleep(poll_int)


# %%
def lift_status_notifier(driver, secrets, phone_number_list, int=300):
    client = twilio_setup(secrets)
    df_before = dl_lift_status(driver)

    while is_now_in_time_period(
        start_time=dt.time(5, 30),
        end_time=dt.time(17, 30),
        now_time=dt.datetime.now().astimezone(pytz.timezone("US/Pacific")).time(),
    ):
        df_now = dl_lift_status(driver)
        lift_update_str = check_for_lift_status_change(df_before, df_now)
        df_before = df_now  # reset comparison
        print(df_before)
        print(dt.datetime.now())
        if lift_update_str:
            print(lift_update_str)
            message = client.messages.create(
                body=lift_update_str, from_="+18336914492", to=phone_number_list
            )
        time.sleep(int)


# %%
def lift_status_notification_deploy():
    secrets = import_secrets()
    selenium_service = selenium_setup()
    driver = create_selenium_driver(selenium_service)
    mycursor = conn_to_sql()
    lift_status_notifier(driver, secrets, "+19252075547")


# %%
secrets = import_secrets()
selenium_service = selenium_setup()
driver = create_selenium_driver(selenium_service)
db_conn = conn_to_sql(secrets)
# %%
lift_dat = dl_lift_status(driver)
lift_dat
# %%
wind_dat = dl_wind_dat(driver)
wind_dat
# %%
