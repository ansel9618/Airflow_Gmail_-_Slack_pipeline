# Airflow_Gmail_-_Slack_pipeline
Tools used for Pipeline implementation 🦖

```
Storage      :AWS S3

Warehouse    :Hive

Nosql        :Hbase

Distributed  :Spark
Processing 
System

Distributed  :Itversity
Cluster

Orchestrator : Airflow

Notification :Gmail SMTP_Server/Slack

Editor       :Vscode,intellij

Languages    :Python,sql,scala

```
# Steps to implement pipeline.


## 1) Check if files are available in AWS s3 using airflow sensor.

## 2) If file in S3 download and fetch file from AWS S3 to edge node in Itversity cluster.

## 3) Use Sqoop to fecth customer data from mysql to hive -this is a complete dump with no incremental load and is non partitioned.
     (note: we have to set metastore in hive in Itversity else will get while creating  database refer:https://www.youtube.com/watch?v=N_uz0gcQCIc)
## 4) upload orders file from local -->edgenode --> Hdfs location.

## 5) Create spark jar using scala and intellij ide to filter out records with status 'CLOSED' from orders.csv.

## 6) Once spark jar is pushed to edge node using scp command  use spark submit to filter out records and store them in repo.

## 7) Create hive table from the spark table w.r.t the filtered records.

## 8) Create habse table by joining the customers and orders csv files in hive.

## 9) Use Email operator in airflow to notify user w.r.t the successs/ failure of pipeline.
     (note: make sure to set smtp config in airflow.cfg
          smtp_user     --> your email
          smtp_password --> use the password from gmail generated from  App password (for this u need to enable 2 factor authentication)
                            using your mail password will lead to user credential error
          
           ```
          smtp_host = smtp.gmail.com
          smtp_starttls = True
          smtp_ssl = False
          # Example: smtp_user = airflow
          smtp_user = **************
          # Example: smtp_password = airflow
          smtp_password = **************
          smtp_port = 587
          smtp_mail_from = airflow@example.com
          smtp_timeout = 30
          smtp_retry_limit = 5
           ```

