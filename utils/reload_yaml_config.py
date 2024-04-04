import json
import os
import yaml
from airflow.models import Variable

CONFIG_FILE_PATH = os.environ["AIRFLOW_HOME"] + "/dags/dag_config.yaml"

for dag_id, dag_conf in yaml.safe_load(open(CONFIG_FILE_PATH)).items():
    dag_conf["dag_conf"]["default_args"] = {**dag_conf["dag_conf"]["default_args"]}
    dag_params = {**dag_conf["dag_conf"]}
    default_config = json.loads(json.dumps(dag_params, default=str))
    config = Variable.set(dag_id, default_config, serialize_json=True)
