# gcp-bigquery-airflow
We will be creating Airflow Pipeline for Working on GCP BigQuery for below use cases 

1. Checking data on a BigQuery Table 
2. Running Query on Bigquery and Store the result in new table 
3. Joining two tables and Storing result in new Table

For Each Task we have seperate python script . To run the scripts their is some setup you need to do :

1. Create a local airflow setup or use your already running setup if you have 
2. Create a connection for your GCP account to access big query in your account 
3. Create a dataset in bigquery in your account were the created table will be stored 

We will be querying public dataset on GCP so you dont need to create those tables . 

After completing the above three steps change these 3 variables in the code 

BQ_CONN_ID  - Your connection name which you created in your airflow 
BQ_PROJECT  - Your project name in GCP
BQ_DATASET  - Your dataset name which you created in GCP 

After setting this three variables copy the scipt to your DAG folder of airflow . 

You can test individual tasks in the DAG before running the full DAG in airflow by using the following command :

airflow test <dag_name> <taskname> <date>
e.g for 1st task  
airflow test bigquery_check bq_check_githubarchive_day 2020-12-01
