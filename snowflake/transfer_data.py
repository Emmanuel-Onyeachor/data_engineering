# install boto3 and snowflake-connector-python libraries

# import libraries
import os
import boto3
import snowflake.connector
from botocore.client import Config
from dotenv import load_dotenv  

load_dotenv()

# define minio s3 credentials
MINIO_ENDPOINT = "https://play.min.io:9443/browser/"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = "housing-project-test"
OBJECT_KEY = "housing.csv"

# define snowflake credentials
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = "housing_database"
SNOWFLAKE_SCHEMA = "housing_schema"
SNOWFLAKE_WAREHOUSE = "housing_warehouse"
SNOWFLAKE_STAGE = "housing_stage"

# initialise minio s3 client
s3 = boto3.client(
    's3',
    endpoint_url = MINIO_ENDPOINT,
    aws_access_key_id = MINIO_ACCESS_KEY,
    aws_secret_access_key = MINIO_SECRET_KEY,
    config = Config(signature_version='s3v4', s3={'addressing_style':'path'})
)

# initialise snowflake connection
conn = snowflake.connector.connect(
    user = SNOWFLAKE_USER,
    password = SNOWFLAKE_PASSWORD,
    account = SNOWFLAKE_ACCOUNT,
    warehouse = SNOWFLAKE_WAREHOUSE,
    database = SNOWFLAKE_DATABASE,
    schema = SNOWFLAKE_SCHEMA,
    client_session_keep_alive = True,
    network_timeout = 60
)

# define function to upload from minio to snowflake
def transfer_data():
    # download the data from minio
    try:
        file_path = "/tmp/" + OBJECT_KEY.split('/')[-1]
        s3.download_file(BUCKET_NAME, OBJECT_KEY, file_path)
        print(f"Download {OBJECT_KEY} from MinIO to {file_path}")
    except Exception as e:
        print(f"Error: {e}")

    # upload data to snowflake stage
    # with conn.cursor() as cur:
    #     cur.execute(f"PUT file://{file_path} @{SNOWFLAKE_STAGE}")
    #     print(f"Uploaded {file_path} to Snowflake stage @{SNOWFLAKE_STAGE}")

    with conn.cursor() as cur:
        cur.execute(f"""
            COPY INTO housing
            FROM @HOUSING_DATABASE.HOUSING_SCHEMA.HOUSING_STAGE/tmp/housing.csv
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        """)
        print("Data loaded into your_table_name from external stage.")


    # # Load data from snowflake stage into table
    #     cur.execute(f"""
    #         COPY INTO housing
    #         FROM @{SNOWFLAKE_STAGE}/{OBJECT_KEY.split('/')[-1]}
    #         FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
    #     """)
    #     print("Data loaded into Snowflake table")

if __name__ == "__main__":
    transfer_data()
