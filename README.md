# Covid Analysis DAG


#### Prerequisites
Install prefect

    pip install -r requirements.txt

#### Configurations

Update conf/config.toml

    [databricks]
    # Existing cluster id for computation
    existing_cluster_id = '0616-121029-3n0ot9ei'
    # Full notebook path
    notebook_path = '/Repos/mahendra.z@sigmoidanalytics.com/prefect-databrick/src/databricks_notebooks/' 
    # dbfs storage folder path
    dbfs_folder_path = '/FileStore/assignment/mahendra/'
    # data storage csv file name
    data_file_name = 'latest-covid-data.csv'
    # result storage csv file name
    result_file_name = 'covid-analysis-result.csv'pip install prefect
    
Export secrets
Databricks connection string

    export PREFECT__CONTEXT__SECRETS__DATABRICKS_CONNECTION_STRING='{"host":"adb-*.azuredatabricks.net","token":"**-3"}'

Rapid API Key
    
    export PREFECT__CONTEXT__SECRETS__XRAPIDAPIKEY='**'

#### Run

    sh stop.sh
    
#### Stop

    sh stop.sh
