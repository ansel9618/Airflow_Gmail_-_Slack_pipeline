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

Notification :Gmail/Slack

Editor       :Vscode,intellij

Languages     :Python,sql,scala

```
# Steps to implement pipeline


## 1) Check if files are available in AWS s3 using airflow sensor

## 2) If file in S3 download and fetch file from AWS S3 to edge node in Itversity cluster

## 3) Use Sqoop to fecth customer data from mysql to hive -this is a complete dump with no incremental load and is non partitioned
     (note: we have to set metastore in hive in Itversity else will get while creating  database refer:https://www.youtube.com/watch?v=N_uz0gcQCIc)
## 4) upload orders file from local -->edgenode --> Hdfs location

## 5) create spark jar using scala and intellij ide to filter out records with status 'CLOSED' from orders.csv

## 6) Once spark jar is pushed to edge node using scp command  use spark submit to filter out records and store them in repo

## 7) create hive table from the spark table w.r.t the filtered records

## 8) Create habse table by joining the customers and orders csv files in hive 

## 9)use Email operator in airflow to notify user w.r.t the successs/ failure of pipeline

