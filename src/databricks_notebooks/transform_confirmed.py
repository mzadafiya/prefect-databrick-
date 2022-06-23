# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Task-3 Country with highest covid cases
# MAGIC
# MAGIC Find out country with highest confirmed covid cases.

# COMMAND ----------

import os
import json
from pyspark.sql.functions import col
from setup_logger import create_logger

# COMMAND ----------

logger = create_logger(task='Data Transform: Covid Confirmed Cases')
logger.info('Starting task run...')

# COMMAND ----------

logger.info('Reading widgets(passed parameters) dbfs_file_path, dbfs_response_file_path')

dbfs_file_path = dbutils.widgets.get('dbfs_file_path')
dbfs_response_file_path = dbutils.widgets.get('dbfs_response_file_path')

dbfs_response_filename = '/dbfs%s' % dbfs_response_file_path

# COMMAND ----------

# File type
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(dbfs_file_path)

display(df)

# COMMAND ----------

logger.info('Creating new dataframe: sorting confirmed in descending order')

sort_by_total_cases_df = df.sort(col("confirmed").desc()).collect()

# COMMAND ----------

data = {
    'country': sort_by_total_cases_df[0][2],
    'kpi': 'Confirmed cases',
    'value': sort_by_total_cases_df[0][5],
    'degree': 'highest'
}

with open(dbfs_response_filename, 'w+') as f:
    json.dump(data, f)

# COMMAND ----------

if not os.path.exists(dbfs_response_filename):
    raise Exception('Upload result to dbfs file is failed. A file does not exist')

logger.info('Upload result to  dbfs file is successful')
