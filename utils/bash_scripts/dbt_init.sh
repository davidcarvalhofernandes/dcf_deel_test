#!/bin/bash

export DBT_PROFILES_DIR=$AIRFLOW_HOME/dbt/.dbt
dbt deps --project-dir $AIRFLOW_HOME/dbt --target $WORK_ENV
dbt compile --project-dir $AIRFLOW_HOME/dbt --target $WORK_ENV
dbt seed --project-dir $AIRFLOW_HOME/dbt --target $WORK_ENV --full-refresh
