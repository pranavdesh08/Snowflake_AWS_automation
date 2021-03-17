from datetime import datetime , date, timedelta
from AWS_connection import aws_pre_trade, aws_post_trade # custom module
from Gcloud_Snowflk import file_to_snow # custom module
from configurations import ftp_conn
from google.cloud import storage
import pysftp
import re
import os

bucket_name = 'bondcliq-msg-files' # change the bucket name 

yesterday =datetime.today().replace(hour=0, minute=00, second=00,microsecond=0)
date = str(yesterday.year).zfill(2)+str(yesterday.month).zfill(2)+str(yesterday.day).zfill(2)

aws_date = str(yesterday.year).zfill(2)+'-'+str(yesterday.month).zfill(2)+'-'+str(yesterday.day).zfill(2)

# pre-trade file
lkupfile_pre_trade = "bcliq_quotes_2100-"+date  # name of the file located in local server
# post-trade file
lkupfile_post_trade = "bcliq_msgs_1800-"+date

# connection to FTP and Google cloud
GOOGLE_APPLICATION_CREDENTIALS = ftp_conn['GOOGLE_APPLICATION_CREDENTIALS']
ftpHost= ftp_conn['ftpHost']
ftpUsername= ftp_conn['ftpUsername']
ftpPassword= ftp_conn['ftpPassword']

def main():
    get_Pre_Trades()
    upload_blob(bucket_name,lkupfile_pre_trade,lkupfile_pre_trade)
    file_to_snow('pretrade',lkupfile_pre_trade)
    aws_pre_trade(lkupfile_pre_trade, aws_date)
  
    get_Post_Trades()
    upload_blob(bucket_name,lkupfile_post_trade,lkupfile_post_trade)
    file_to_snow('post_trade',lkupfile_post_trade)
    aws_post_trade(lkupfile_post_trade, aws_date)

    clean_files()

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """This funtion is used to load the file into specific Google bucket. 
    Establish connection to GCP using correct credentials """
    
    if re.search("^bcliq_quotes", source_file_name):
        bucket_name = 'bondcliq-quotes-files'

    """Uploads a file to the bucket."""
    storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))

def get_Pre_Trades():
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
	# Define the file that you want to download from the remote directory
    print(lkupfile_pre_trade)
    with pysftp.Connection(host=ftpHost, username=ftpUsername, password=ftpPassword,cnopts=cnopts) as sftp:
	remoteFilePath = "/upload/"+lkupfile_pre_trade
    	# Define the local path where the file will be saved
	localFilePath = lkupfile_pre_trade
	sftp.get(remoteFilePath, localFilePath)

def get_Post_Trades():
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
  # Define the file that you want to download from the remote directory
    print(lkupfile_post_trade)
    with pysftp.Connection(host=ftpHost, username=ftpUsername, password=ftpPassword,cnopts=cnopts) as sftp:	
	remoteFilePath = "/upload/"+lkupfile_post_trade
    	# Define the local path where the file will be saved
	localFilePath = lkupfile_post_trade
	sftp.get(remoteFilePath, localFilePath)

def explicit():
    from google.cloud import storage
    print("Autheticating into Google Cloud")
    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
    buckets = list(storage_client.list_buckets())
    print(buckets)

def clean_files():
    if os.path.exists(lkupfile_post_trade):
        os.remove(lkupfile_post_trade)
    if os.path.exists(lkupfile_pre_trade):
        os.remove(lkupfile_pre_trade)

if  __name__ == '__main__':
	main()


