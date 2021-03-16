import snowflake.connector
from configurations import snow_conn

""" This script is use to load data into Snowflake database using python"""

# Connect to Snowflake account 
conn = snowflake.connector.connect(
                user= snow_conn['Username'],
                password= snow_conn['Password'] ,
                account=snow_conn['Account'],
                warehouse=snow_conn['Warehouse'],
                database=snow_conn['Database'],
                schema=snow_conn['Schema'],
                role =snow_conn['Role']
                )
# Execute a query
def execute_query(connection,query):
        cur = connection.cursor()
        cur.execute(query)
        cur.close()

def file_to_snow(table_name, file_name):
        # Specify the Db to be used
        sql = 'use database EOD_BONDS'
        execute_query(conn,sql)

        # Stage a file to Snowflake  
        csv_file = file_name # replace with file name from Gcloud
        sql = 'put file://{} @my_python_stage auto_compress= true'.format(csv_file)
        execute_query(conn,sql)

        #replace with the file name. Testing purpose
        # if table_name == 'pretrade':
        #         file_name = 'bcliq_quotes_2100-20210310'
        # else:
        #         file_name = 'bcliq_msgs_1800-20210310'

        # Load the staged file into Snowflake tables.
        sql = 'copy into {} from @my_python_stage/{}.gz file_format = "CSV" on_error = continue'.format(table_name, file_name)
        execute_query(conn,sql)

        #Clear all the staged files.
        sql = 'remove @my_python_stage pattern=".*.gz";'
        execute_query(conn,sql)

        return 'Successfully uploaded file {} to {}'.format(file_name,table_name)

