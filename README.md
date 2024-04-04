This repo has the code to answer the questions asked in Deel's take home exercise.
Since the last question suggests to use Airflow to send a Slack notification, I used an airflow docker image to create a DAG that runs the dbt models created, the correspodning tests and also a validation task to see if a particular test has failed and send a Slack notification if so.
The DAG looks like this:
![image](https://github.com/davidcarvalhofernandes/dcf_deel_test/assets/60223435/90580602-17a5-427f-b8b4-7bb4c78055b0)


Instructions to run the code:
  1. Add you slack token in docker-compose.yml
  2. Run `docker-compose up --build`

Comments and assumptions:
Question #1: Dimension table for organizations enriched with important information
I created d_organizations for this. The only additional information I added was the column created_date_ts which is just the original column created_date in a timestamp format.

Question #2: Fact table at date / organization_id granularity
Created f_organizations_daily for this purpose. AS there were no invoice_dates in the data set provided, I also created s_invoice_dates_mapping to generate random dates for the invoices. For each parent_invoice_id, I checked the created_date of the organization and created a random date in the interval created_date <= x <= created_date +100 days.

Question #3: Tests to ensure data quality is accurate
I added some very basic tests in core.yml plus one custom test in the tests folder that was needed for question 4.

Question #4: I used airflow for this.
