import prefect
from prefect import Flow
from prefect.tasks.databricks.databricks_submitjob import DatabricksSubmitRun
from prefect.client import Secret
from prefect.executors import LocalDaskExecutor

# Read secrets
databricks_conn_secret = Secret("DATABRICKS_CONNECTION_STRING").get()
x_rapid_api_key = Secret("XRAPIDAPIKEY").get()

# Read databricks config
existing_cluster_id = prefect.config.databricks.existing_cluster_id
notebook_path = prefect.config.databricks.notebook_path
dbfs_folder_path = prefect.config.databricks.dbfs_folder_path
data_file_name = prefect.config.databricks.data_file_name
result_file_name = prefect.config.databricks.result_file_name

# Read API config
api_host = prefect.config.api.host
api_url = prefect.config.api.url
countries = prefect.config.api.countries


def get_submit_config(notebook_name, notebook_params):
    """
    SubmitRun config template for the DatabricksSubmitRun task,
    Spark Python Task params must be passed as a list.
    """
    return {
        "run_name": "Job-" + notebook_name,
        "existing_cluster_id": "0616-121029-3n0ot9ei",
        "notebook_task": {
            "notebook_path": notebook_path + notebook_name,
            "base_parameters": notebook_params,
            "source": "WORKSPACE",
        },
    }


def get_dbfs_path(file_name):
    """append filename to dbfs path

    Args:
        file_name (string): file name

    Returns:
        string: dbfs_path
    """
    return "%s%s" % (dbfs_folder_path, file_name)


with Flow("Databricks-Tasks", schedule=None) as flow:
    """
    DAG structure
    1. Data extract
    2. Data Transform
    3. Data Load
    """

    ## 1. Data extract
    extract = DatabricksSubmitRun(
        name="Data Extract",
        databricks_conn_secret=databricks_conn_secret,
        json=get_submit_config(
            "extract",
            {
                "x_rapid_api_key": x_rapid_api_key,
                "countries": countries,
                "dbfs_file_path": get_dbfs_path(data_file_name),
                "api_host": api_host,
                "api_url": api_url
            },
        ),
    )

    ## 2. Data Transform
    transform_confirmed = DatabricksSubmitRun(
        name="Data Transform: Confirmed Cases",
        databricks_conn_secret=databricks_conn_secret,
        json=get_submit_config(
            "transform_confirmed",
            {
                "dbfs_file_path": get_dbfs_path(data_file_name),
                "dbfs_response_file_path": get_dbfs_path("temp/transform_confirmed_res.csv"),
            },
        ),
    )

    transform_efficiency = DatabricksSubmitRun(
        name="Data Transform: Covid Efficiency",
        databricks_conn_secret=databricks_conn_secret,
        json=get_submit_config(
            "transform_efficiency",
            {
                "dbfs_file_path": get_dbfs_path(data_file_name),
                "dbfs_response_file_path": get_dbfs_path("temp/transform_efficiency_res.csv"),
            },
        ),
    )

    transform_impact = DatabricksSubmitRun(
        name="Data Transform: Covid Impact",
        databricks_conn_secret=databricks_conn_secret,
        json=get_submit_config(
            "transform_impact",
            {
                "dbfs_file_path": get_dbfs_path(data_file_name),
                "dbfs_response_file_path": get_dbfs_path("temp/transform_impact_res.csv"),
            },
        ),
    )

    ## 3. Data Load
    load = DatabricksSubmitRun(
        name="Data Load",
        databricks_conn_secret=databricks_conn_secret,
        json=get_submit_config("load", {"dbfs_file_path": get_dbfs_path(result_file_name), "dbfs_response_files": get_dbfs_path("temp")}),
    )

    # Add edges in the flow between tasks
    # All transformation tasks can be performed in parallel
    flow.add_edge(extract, transform_confirmed)
    flow.add_edge(extract, transform_efficiency)
    flow.add_edge(extract, transform_impact)
    flow.add_edge(transform_confirmed, load)
    flow.add_edge(transform_efficiency, load)
    flow.add_edge(transform_impact, load)

# LocalDaskExecutor, an executor that runs on dask primitives with a using either threads or processes.
flow.executor = LocalDaskExecutor()

# flow.register('YOUR_PROJECT') to register your flow on the UI
flow.register(project_name = 'Covid Analysis')
