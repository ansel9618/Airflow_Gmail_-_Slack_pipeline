from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow import settings
from airflow.models import Connection
from airflow.operators.email_operator import EmailOperator




#creating initial dag
dag = DAG(
    dag_id = 'customer_360_pipeline',
    start_date = days_ago(1)
    
)

#creating a http sensor to check whether file resides in s3
sensor = HttpSensor(
    task_id = 'watch_for_orders_at_s3',
    http_conn_id = 'order_s3',
    endpoint = 'orders.csv',
    response_check = lambda response: response.status_code==200,
    retry_delay=timedelta(minutes=5),
    retries=12,
    dag=dag
)

# downloading file in s3 to edge node in itversity cluster

#this function enable us to get the connection string created in connection id
#this prevents had coding for this we need to create a session and filter the respective
#coneection_id
def get_order_url():
    session = settings.Session()
    connection = session.query(Connection).filter(Connection.conn_id == 'order_s3').first()
    return f'{connection.schema}://{connection.host}/orders.csv'


download_order_cmd = f'rm -rf airflow_pipeline && mkdir -p airflow_pipeline && cd airflow_pipeline && wget {get_order_url()}'

download_to_edgenode = SSHOperator(
    task_id = 'download_orders',
    ssh_conn_id = 'itversity',
    command =download_order_cmd,
    dag=dag
)

#---using sqoop to fetch customers from mysql db in itversity and dumping to hive here we are doing a complete dump with no incremental loads and table is non partitioned---
#here we already have a built in customer table in retail_db in mysql and can be looked at via using sqoop commands
#"sqoop-list-tables --connect "jdbc:mysql://g02.itversity.com:3306/retail_db" --username retail_user --password itversity

#also to see contents we can use
#sqoop-eval --connect "jdbc:mysql://g02.itversity.com:3306/retail_db" --username retail_user --password itversity --query "select * from retail_db.customers limit 10";

#first u need to create a databse in hive so we have to set the metastore
#https://www.youtube.com/watch?v=N_uz0gcQCIc -->refer video to set metastore else commands will fail
#data base created with name airflow_ansel

#in sqoop command do not give password directly in production cluster make sure to use connection as used above
#ideally its good to do the clean upif we creating any tables making sure table with same name dosent exist

def fetch_customer_info_cmd():
    command_one =  "hive -e 'DROP TABLE airflow_ansel.customers'"
    command_two = "sqoop import --connect \"jdbc:mysql://g02.itversity.com:3306/retail_db\" \
        --username retail_user --password itversity --table customers --hive-import --hive-database airflow_ansel --hive-table customers"
    return f'{command_one} && {command_two}'

# ssh connection to the itversity gateway edge node and run sqoop command
import_customer_info = SSHOperator(
    task_id = 'download_customers',
    ssh_conn_id = 'itversity',
    command =fetch_customer_info_cmd(),
    dag=dag
)

#uploading orders file to hdfs(hdfs command execution)
#here -p make sure that command does not fail when folder with airflow_input exist it says its fine
upload_orders_to_hdfs = SSHOperator(
    task_id = 'upload_orders_to_hdfs',
    ssh_conn_id = 'itversity',
    command = 'hdfs dfs -rm -R -f airflow_input && hdfs dfs -mkdir -p airflow_input && hdfs dfs -put ./airflow_pipeline/orders.csv airflow_input',
    dag=dag
)

#spark pgm execution with jar file uploaded 
def get_order_filter_cmd():
	command_zero='export SPARK_MAJOR_VERSION=2'
	command_one='hdfs dfs -rm -R -f airflow_output'
	command_two = 'spark-submit --class filterOrders --master yarn --deploy-mode cluster /home/itv003409/bigdata_scala.jar /user/itv003409/airflow_input/orders.csv /user/itv003409/airflow_output'
	return f'{command_zero} && {command_one} && {command_two}' 

#spark execution
#cmd_timeout is to make sure that there is commnad line by default times out at 10 sec so we extend to 30 sec
process_order_info=SSHOperator(
	task_id='process_orders',
	ssh_conn_id='itversity',
	command=get_order_filter_cmd(),
    cmd_timeout=30,
	dag=dag
	)


#create_hive_table_command

def create_hive_orders_table_cmd():
	command_one = '''hive -e "CREATE external table if not exists airflow_ansel.orders(order_id int,order_date string,order_customer_id int,order_status string) row format delimited fields terminated by ',' stored as textfile location '/user/itv003409/airflow_output' "'''
	return f'{command_one}'

create_hive_order_table=SSHOperator(
	task_id='create_hive_order_table',
	ssh_conn_id='itversity',
	command=create_hive_orders_table_cmd(),
    cmd_timeout=30,
	dag=dag
	)

#hbase table command
def hbase_table_cmd():
    command_one = '''hive -e "CREATE table if not exists airflow_ansel.airflow_hbase(customer_id int, customer_fname string, customer_lname string, order_id int, order_date string ) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' with SERDEPROPERTIES('hbase.columns.mapping'=':key,personal:customer_fname, personal:customer_lname, personal:order_id, personal:order_date')"'''
    command_two = 'hive -e "insert overwrite table airflow_ansel.airflow_hbase select c.customer_id, c.customer_fname, c.customer_lname, o.order_id, o.order_date from airflow_ansel.customers c join airflow_ansel.orders o ON (c.customer_id = o.order_customer_id)"'
    return f'{command_one} && {command_two}' 

# hbase table creation
load_hbase = SSHOperator(
    task_id='load_hbase_table',
    ssh_conn_id='itversity',
    cmd_timeout=30,
    command=hbase_table_cmd(),
    dag=dag
)


# dummy = DummyOperator(
#     task_id='dummy',
#     dag=dag
# )

#make sure settings are configured in airflow.config
#also make sure to generate app password from gmail(for this enable 2 factor authentication)
success_notify = EmailOperator(
    task_id='sucess_email_notify',
    to='gowk510@gmail.com',
    subject='Pipeline Execution Success',
    html_content=""" <h1>Great job :) data has been loaded to hbase table.</h1> """,
    trigger_rule='all_success',
    dag=dag
)

failure_notify = EmailOperator(
    task_id='failure_email_notify',
    to='gowk510@gmail.com',
    subject='Pipeline Execution failed',
    html_content=""" <h1>Sorry :( Failed to load data into hbase table .</h1> """,
    trigger_rule='all_failed',
    dag=dag
)




sensor >>  import_customer_info 
sensor >> download_to_edgenode >> upload_orders_to_hdfs >> process_order_info >> create_hive_order_table
[import_customer_info,create_hive_order_table] >> load_hbase >> [success_notify,failure_notify]