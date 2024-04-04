import json
import os
import yaml

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.models import Variable


from typing import Any, Dict

# 1) DAG Info
DAG_PATH = os.path.dirname(__file__)
DBT_MODEL_PATH = os.environ["AIRFLOW_HOME"] + "/dbt_deel_test/models"
DBT_PROFILES_PATH = os.environ["AIRFLOW_HOME"] + "/dbt_deel_test/.dbt"
DBT_RUN_RESULTS_PATH = (
    os.environ["AIRFLOW_HOME"] + "/dbt_deel_test/target/run_results.json"
)
DAG_NAME_PREFIX = "dbt_load"


# Load the manifest file that stores the dbt DAG and nodes information
def load_manifest():
    local_filepath = os.environ["AIRFLOW_HOME"] + "/dbt_deel_test/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data


# This function parses the run_results.json artifact for a given dbt test and sends a Slack notification if there are failures
def parse_run_results_and_slack_alert(test_id, **context):
    # Read the run results file
    with open(DBT_RUN_RESULTS_PATH) as f:
        data = json.load(f)
        test_id_unique_id = test_id
        failures_value = 0
        # Iterate through the results to find the test with the specified ID
        for item in data.get("results"):
            if item.get("unique_id") == test_id_unique_id:
                failures_value = item.get("failures")
                compiled_code = item.get("compiled_code")
                break

    # Prepare the Slack notification message
    body_slack = f"""
:red_circle: Airflow Task Failed:
*DAG:* {context.get('task_instance').dag_id}
*Task:* {context.get('task_instance').task_id}  
*Task instance start date*: {context.get('task_instance').start_date}
*Task instance end date:* {context.get('task_instance').end_date}
*Task instance duration (mins):* {context.get('task_instance').duration}
*Run ID:* {context.get('run_id')}
*Log URL:* <{context.get('task_instance').log_url}|{context.get('task_instance').task_id}>
*Exception:* The dbt test check_daily_ratio_above_50pct has failed with {failures_value} failures. This means there are accounts with balance changes over 50%. 
Please check the query: ``` {compiled_code} ``` for more details.

Have a nice day, 
Airflow bot
"""

    # Send the Slack notification if there are failures
    if failures_value > 0:
        send_slack_notification(
            text=body_slack,
            channel="#dcf-test-slack",
            slack_conn_id="slack_conn",
        ).notify(context)
    return failures_value


def create_dag(dag_id: str, dag_conf: Dict[str, Any]):
    default_args_interpreted = {}
    for arg_key, arg_value in dag_conf.get("default_args").items():
        default_args_interpreted[arg_key] = arg_value
    dag = DAG(
        dag_id=dag_id,
        description=dag_conf["description"],
        default_args=default_args_interpreted,
        schedule_interval=dag_conf["schedule_interval"],
        catchup=False,
        tags=dag_conf.get("tags"),
    )

    return dag


for dag_id, dag_conf in yaml.safe_load(open(f"{DAG_PATH}/dag_config.yaml")).items():
    dag_config = Variable.get(dag_id, default_var=None, deserialize_json=True)
    globals()[dag_id] = create_dag(dag_id, dag_config)


class RunModelBashOperator(BashOperator):
    ui_color = "#8BD1F5"
    ui_fgcolor = "white"


class TestBashOperator(BashOperator):
    ui_color = "grey"
    ui_fgcolor = "white"
    xcom_push = True


def make_dbt_task(node, node_folder, dag_config, dbt_verb, extra_params={}):
    """Returns an Airflow operator either run and test an individual model"""
    DBT_DIR = os.environ["AIRFLOW_HOME"] + "/dbt_deel_test/"
    # GLOBAL_CLI_FLAGS = "--no-write-json"
    GLOBAL_CLI_FLAGS = ""
    dag_id = f"{DAG_NAME_PREFIX}_{node_folder}"
    model = node.split(".")[-1]
    dag_query_params = dag_config.get("query_params")

    if dbt_verb == "run":
        dbt_task = RunModelBashOperator(
            task_id=node,
            bash_command=f"""
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --project-dir '{DBT_DIR}' --profiles-dir '{DBT_PROFILES_PATH}' --target dev --select {model}
            """,
            dag=globals()[dag_id],
        )
    elif dbt_verb == "test":
        node_test = node.replace("model", "test")
        dbt_task = TestBashOperator(
            retries=0,
            task_id=node_test,
            bash_command=f"""
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --project-dir '{DBT_DIR}' --profiles-dir '{DBT_PROFILES_PATH}' --target dev --select {model} --indirect-selection=cautious
            """,
            dag=globals()[dag_id],
        )
    return dbt_task


data = load_manifest()

dbt_tasks = {}
models_with_test = []


# Get the dependencies for the test nodes
for node_key, node_val in data["nodes"].items():
    if node_val.get("resource_type") != "seed":
        for node_dependency in node_val.get("depends_on").get("nodes"):
            if node_dependency[:5] == "model" and node_key[:4] == "test":
                models_with_test.append(node_dependency)


for node_key, node_val in data["nodes"].items():
    node_folder = node_val.get("path").split("/")[0]
    dag_id = f"{DAG_NAME_PREFIX}_{node_folder}"
    if globals().get(dag_id):
        node_dag_config = Variable.get(dag_id, default_var=None, deserialize_json=True)
        if node_key.split(".")[0] == "model":
            dbt_tasks[node_key] = make_dbt_task(
                node_key, node_folder, node_dag_config, "run"
            )
            if node_key in models_with_test:
                node_test = node_key.replace("model", "test")
                dbt_tasks[node_test] = make_dbt_task(
                    node_key, node_folder, node_dag_config, "test"
                )

for node_key, node_val in data["nodes"].items():
    if node_key.split(".")[0] == "model":
        # Set dependency to run tests on a model after model runs finishes
        if node_key in models_with_test:
            node_test = node_key.replace("model", "test")
            dbt_tasks[node_key] >> dbt_tasks[node_test]
            for node_key1, node_val1 in data["nodes"].items():
                if node_val1.get("resource_type") == "test":
                    for dependency in node_val1.get("depends_on").get("nodes"):
                        test = False
                        if dependency == node_key:
                            test = True
                        if test and "slack_test" in node_val1.get("tags"):
                            node_folder = node_val.get("path").split("/")[0]
                            dag_id = f"{DAG_NAME_PREFIX}_{node_folder}"
                            print(dag_id)
                            if globals().get(dag_id):
                                node_dag_config = Variable.get(
                                    dag_id, default_var=None, deserialize_json=True
                                )
                                op_parse_run_results_and_slack_alert = PythonOperator(
                                    task_id=f"parse_run_results_and_slack_alert_{node_key1}",
                                    python_callable=parse_run_results_and_slack_alert,
                                    trigger_rule="all_done",
                                    dag=globals()[dag_id],
                                    op_kwargs={
                                        "test_id": node_key1,
                                    },
                                )
                                (
                                    dbt_tasks[node_test]
                                    >> op_parse_run_results_and_slack_alert
                                )

        for upstream_node in data["nodes"][node_key]["depends_on"]["nodes"]:

            upstream_node_type = upstream_node.split(".")[0]
            if upstream_node_type == "model":
                dbt_tasks[upstream_node] >> dbt_tasks[node_key]
