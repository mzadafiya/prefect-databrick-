# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Data Transform: Efficiency
# MAGIC
# MAGIC Find out country with highest efficiency(recovered/confirmed).

# COMMAND ----------

import os
import json
from pyspark.sql.functions import col, round
from setup_logger import create_logger

# COMMAND ----------

logger = create_logger(task='Data Transform: Covid Efficiency')
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

logger.info('Creating dataframe from dbfs file')

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(dbfs_file_path)

display(df)

# COMMAND ----------

logger.info('Creating new dataframe: calculating efficiency(recovered/confirmed) and sorting by it in descending order')

efficientdf = df \
    .withColumn("efficiency", round(col("recovered")/col("confirmed"), 4)) \
    .sort(col("efficiency").desc()).collect()

# COMMAND ----------

data = {
    'country': efficientdf[0][2],
    'kpi': 'Efficiency(recovered/confirmed)',
    'value': efficientdf[0][9],
    'degree': 'highest'
}

with open(dbfs_response_filename, 'w+') as f:
    json.dump(data, f)

# COMMAND ----------

if not os.path.exists(dbfs_response_filename):
    raise Exception('Upload result to dbfs file is failed. A file does not exist')

logger.info('Upload result to dbfs file is successful')
