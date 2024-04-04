#!/bin/bash

export DBT_PROFILES_DIR=$AIRFLOW_HOME/dbt/.dbt
dbt deps --project-dir $AIRFLOW_HOME/dbt --target dev
dbt compile --project-dir $AIRFLOW_HOME/dbt --target dev
# dbt seed --project-dir $AIRFLOW_HOME/dbt --target dev --full-refresh
