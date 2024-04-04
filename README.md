This repo has the code to answer the questions asked in Deel's take home exercise.
Since the last question suggests to use Airflow to send a Slack notification, I used an airflow docker image to create a DAG that runs the dbt models created, the correspodning tests and also a validation task to see if a particular test has failed and send a Slack notification if so.
The DAG file is in `dags/dbt_replicate_dag.py` and the graph looks like this:
![image](https://github.com/davidcarvalhofernandes/dcf_deel_test/assets/60223435/90580602-17a5-427f-b8b4-7bb4c78055b0)


Instructions to run the code:
  1. Add you slack token in docker-compose.yml
  2. Add your profile details in `dbt_deel_test/.dbt/profiles_sample.yml` and rename it to `dbt_deel_test/.dbt/profiles.yml`
  3. Run `docker-compose up --build`
  4. Open localhost:8080 on your web browser and you'll see airflow's web interface. The credentials to log in are airflow/airflow.

Comments and assumptions:
Question #1: Dimension table for organizations enriched with important information
I created d_organizations for this. The only additional information I added was the column created_date_ts which is just the original column created_date in a timestamp format. There was no context on what an organization was and adding more data in this dataset seemed unnecessary. 

Question #2: Fact table at date / organization_id granularity
Created f_organizations_daily for this purpose. AS there were no invoice_dates in the data set provided, I also created s_invoice_dates_mapping to generate random dates for the invoices. For each parent_invoice_id, I checked the created_date of the organization and created a random date in the interval created_date <= x <= created_date +100 days. Please note that the table dwh_stg.map_invoice_dates referenced in this query is just a static version of s_invoice_dates_mapping that I executed once to avoid having different random dates in each execution.


Question #3: Tests to ensure data quality is accurate
I added some very basic tests in core.yml plus one custom test in the tests folder that was needed for question 4.

Question #4: I used airflow for this and the native function `send_slack_notification` from `airflow.providers.slack.notifications.slack`. The slack channel is hardcoded in `dags/dbt_replicate_dag.py` so you'll need to change it there for this to work properly on your environment.
Please note that I defined the balance as `total_paid_amount_usd - total_refunded_amount_usd`, meaning the amount of invoices with status "paid" minus the amount of invoices with status "refunded" and comparing this value with the correspondent value of dt -1. This is a simplified approach that might not be accurate functionally but technically the strategy would be the same. 
