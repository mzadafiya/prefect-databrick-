# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Data Extract
# MAGIC
# MAGIC Make a request to https://rapidapi.com/ShubhGupta/api/covid19-data/ for atleast 20 countries using '/GetByCountryName' API, fill up the csv file on DBFS

# COMMAND ----------

import os
import json
import requests
import pandas as pd
from setup_logger import create_logger

# COMMAND ----------

logger = create_logger(task='Data Extract')
logger.info('Starting task run...')

# COMMAND ----------

logger.info('Reading widgets(passed parameters) x_rapid_api_key, countries, dbfs_file_path, api_host and api_url')

countries = dbutils.widgets.get('countries').split(',')
dbfs_file_path = dbutils.widgets.get('dbfs_file_path')

x_rapid_api_key = dbutils.widgets.get('x_rapid_api_key')
api_host = dbutils.widgets.get('api_host')
api_url = dbutils.widgets.get('api_url')

headers = {
    'X-RapidAPI-Host': api_host,
    'X-RapidAPI-Key': x_rapid_api_key
}

# COMMAND ----------

# Check if COVID API is available

error = None
try:
    response = requests.request(
        'GET', api_url, headers=headers, params={"country": 'dummy'})
except requests.exceptions.HTTPError as errh:
    error = "Http Error: %s" % errh
except requests.exceptions.ConnectionError as errc:
    error = "Error Connecting: %s" % errc
except requests.exceptions.Timeout as errt:
    error = "Timeout Error: %s" % errt
except requests.exceptions.RequestException as err:
    error = "Unknown Error" % err

if error:
    raise Exception("API is not available. %s" % error)

# COMMAND ----------


def get_covid_data_by_country(country):
    """
    Make request to covid API and return response and error if any 

    Args:
        country (string): country name

    Returns:
        data: response data

        status_code == 200, return response
        status_code != 200, log error message
    """

    # Query string
    querystring = {"country": country}

    data = None

    logger.info('Request for %s: Fetching started', country)
    response = requests.request(
        'GET', api_url, headers=headers, params=querystring)
    if response.status_code == 200:
        data = json.loads(response.text)
        logger.info('Request for %s: Request successful', country)
    else:
        logger.error('Request for %s: Request failed. Invalid status code %s. Error: %s' %
                     (country, response.status_code, response.text))
    return data

# COMMAND ----------


response_data = []
error_data = []

logger.info('Retrieve latest covid data: Started')
logger.info('Retrieve latest covid data: Making requests for given countries')

# Make request for each countries
for country in countries:
    data = get_covid_data_by_country(country)
    if data:
        response_data.append(data)

if not response_data:
    raise Exception('Retrieve latest covid data: Covid endpoint didn\'t return any data for given countries')

logger.info('Retrieve latest covid data: Completed')

# COMMAND ----------

dbfs_filename = '/dbfs%s' % dbfs_file_path

# create directories if not exist
os.makedirs(os.path.dirname(dbfs_filename), exist_ok=True)

# COMMAND ----------

logger.info('Creating panda dataframe from collected data')

df = pd.read_json(json.dumps(response_data))
df.to_csv(dbfs_filename)

# COMMAND ----------

logger.info('Creating or overwriting dbfs file from panda dataframe')

df = spark.read.format('csv').option('header', 'true').load(dbfs_file_path)
df.show()

# COMMAND ----------

if not os.path.exists(dbfs_filename):
    raise Exception(
        'Collect data to dbfs file failed. A file does not exist')

logger.info('Collect data to dbfs file is successful')
logger.info('Task run successful...')
