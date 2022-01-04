# Spark_SQL-with-Dataproc

## Create SQL instance

in order to create our SQL tables, we need to provision a database first which we will be creating in an SQL engine: **MySQL**

Once we create the SQL server, we then set the tables templates which we are going to feed later 

## Create a bucket

for our staging folder we are creating a storage bucket. Then we load the data files (accomodation & rating) into the bucket

Next, in our SQL engine we will import the two CSV files and feed them to the database we created

## Launch a Dataproc Cluster

we create a Dataproc cluser which will run the spark job later. 
we set the following configurations: 
- Zone: us-central1-a
- For Master node, for Machine type, select n1-standard-2 (2 vCPUs, 7.5 GB memory).
- For Worker nodes, for Machine type, select n1-standard-2 (2 vCPUs, 7.5 GB memory).

Next we need to **Authorizing Cloud Dataproc to connect with Cloud SQL** which is an important step to do 
And finally we should get IP adress for each worker machine in our Dataproc cluster, and give the authorization to those ips to access cloudsql again this is one of the main steps
see the **authorize_vm.sh** file 

## Run the Dataproc 

for us to run the Spark job which is held on the **train_and_apply.py**, we need first to configure the script with our set of options as folloxs: 
- CLOUDSQL_INSTANCE_IP = '104.155.188.32'   # database server IP (we find it in the SQL Server)
- CLOUDSQL_DB_NAME = 'recommendation_spark' # our Database
- CLOUDSQL_USER = 'root'  # the root user
- CLOUDSQL_PWD  = 'root'  # the password for our Cloud SQL

Submit the Pyspark job and specify the python file 

## Explore inserted rows with SQL

Navigate back to the Cloud SQL which we have the results of our recommendations 

**Enjoy**
