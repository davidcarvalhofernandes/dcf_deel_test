#!/bin/bash
echo $AIRFLOW_HOME


(echo "export AIRFLOWMD_USERNAME=airflowmd"; \
echo "export AIRFLOWMD_PASSWORD=airflowmd"; \
echo "export AIRFLOWMD_HOSTNAME=postgres") >> ~/.bash_aliases
. ~/.bash_aliases


(echo "export PYTHONPATH=$AIRFLOW_HOME"; \
 echo "export AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${AIRFLOWMD_USERNAME}:${AIRFLOWMD_PASSWORD}@${AIRFLOWMD_HOSTNAME}:5432/airflowmd")  >> ~/.bash_aliases
. ~/.bash_aliases

airflow version
echo "$AIRFLOW__CORE__SQL_ALCHEMY_CONN"
airflow db init


airflow users create \
    --username airflow \
    --firstname airflow_first_name \
    --lastname airflow_last_name \
    --role Admin \
    --email airflow@test.com \
    --password airflow


## Create connections using the variables above
# airflow connections add --conn-type postgres --conn-host $AIRFLOWMD_HOSTNAME --conn-port $PG_PORT --conn-login $AIRFLOWMD_USERNAME --conn-password $AIRFLOWMD_PASSWORD --conn-schema airflowmd tk_airflow_metadata
airflow connections add --conn-type slack --conn-password $SLACK_TOKEN_DATA_ALERTS slack_conn

python "$AIRFLOW_HOME"/utils/reload_yaml_config.py

#bash
airflow webserver & airflow scheduler
# $AIRFLOW_HOME/utils/bash_scripts/dbt_init.sh & airflow webserver & airflow scheduler
