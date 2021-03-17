# Snowflake_AWS_automation
Code to get files from remote directory using sftp, store the them into GCP buckets, load the data from the files into Snowflake Db, and add the file to AWS S3 bucket. 



Gcloud_Snowflk.py contains code to connect python with Snowflake account/database. Stages the file into Snowflake stage, uploads data, and clears staged files.

upload_trade_file.py call the Gcloud_Snowflk.py used the Snowflake connection to load the data into snowflake database. Connects with the GCP account and loads the data into buckets. Also uses AWS_connection file to connect with AWS account, uploads data in S3 bucket and finalizes the file for Data exchange.
