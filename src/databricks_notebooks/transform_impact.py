# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Data Transform: Impact
# MAGIC
# MAGIC Find out country with highest impact(death/confirmed).

# COMMAND ----------

import os
import json
from pyspark.sql.functions import col, round
from setup_logger import create_logger

# COMMAND ----------

logger = create_logger(task='Data Transform: Covid Impact')
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

logger.info('Creating new dataframe: calculating affected(deaths/confirmed) and sorting by it in descending order')

affected_country = df \
    .withColumn("affected", round(col("deaths")/col("confirmed"), 4)) \
    .sort(col("affected").desc()).collect()

# COMMAND ----------

data = {
    'country': affected_country[0][2],
    'kpi': 'Impact(deaths/confirmed)',
    'value': affected_country[0][9],
    'degree': 'highest'
}

with open(dbfs_response_filename, 'w+') as f:
    json.dump(data, f)

# COMMAND ----------

if not os.path.exists(dbfs_response_filename):
    raise Exception('Upload result to dbfs file is failed. A file does not exist')

logger.info('Upload result to dbfs file is successful')
