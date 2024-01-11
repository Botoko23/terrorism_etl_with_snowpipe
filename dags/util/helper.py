import json
import pytz
from datetime import datetime

import pandas as pd
import warnings
from botocore.exceptions import ClientError
import snowflake.connector



class Transfrom:
    cols_to_use : list[str] = [ 'iyear', 'imonth', 'iday', 'extended', 'resolution', 'country_txt', 'region_txt', 'provstate', 'city', 'latitude',
    'longitude', 'summary', 'crit1', 'crit2', 'crit3', 'doubtterr',
    'success', 'suicide', 'attacktype1_txt', 'targtype1_txt', 'gname', 
    'motive', 'guncertain1', 'individual','nperps', 'nperpcap', 'weaptype1_txt', 
    'weapsubtype1_txt', 'nkill', 'nkillus', 'nkillter', 'nwound', 'nwoundus', 'nwoundte', 
    'propvalue', 'ishostkid', 'nhostkid', 'nhostkidus', 'nhours', 'ndays', 
    'ransom', 'ransomamt', 'ransompaid', 'hostkidoutcome_txt', 'nreleased']
    def __init__(self, *filenames) -> None:
        pd.set_option('display.max_columns', None)
        warnings.filterwarnings('ignore')
        df = pd.read_csv(filenames[0], encoding='latin1', compression='gzip', usecols = self.cols_to_use)
        if len(filenames) > 1:
            for filename in filenames[1:]:
                other_df = pd.read_csv(filename, encoding='latin1', usecols= self.cols_to_use)
                df = pd.concat([df, other_df], ignore_index=True)
            self.dataframe = df
        self.dataframe = df
        
    def add_date_col(self, year_col: str, month_col: str, day_col: str, sep='-'):
        return self.dataframe[year_col].astype('str') + sep + self.dataframe[month_col].astype('str')  + sep\
                + self.dataframe[day_col].astype('str')
    def to_date(self, col_name, format="%Y-%m-%d", on_errors='coerce'):
        self.dataframe[col_name] = pd.to_datetime(self.dataframe[col_name], format=format, errors=on_errors)
    
    def determine_doubtterr(self, *criteria) -> None:
        self.dataframe.loc[(self.dataframe['crit1'] == criteria[0]) &  (self.dataframe['crit2'] == criteria[1]) & \
              (self.dataframe['crit3'] == criteria[2]), 'doubtterr'] = 0
    def fillna_for_certain_cols(self, cols):
        return self.dataframe[cols].fillna(0).astype(int)


def create_file_name(prefix:str, name:str, format:str) -> str:
    now = datetime.now()
    return f'{prefix}{name}_{now.strftime("%Y-%b-%d_%H:%M:%S")}.{format}'


def get_secrets(boto3_client, secret_name):
  
    try:
        response = boto3_client.get_secret_value(
            SecretId=secret_name
        )

        secret_data = json.loads(response['SecretString'])
    
        for key, value in secret_data.items():
            print(f"Key: {key}, Value: {value}")
    except ClientError as e:
        raise e
    
    finally:
        return secret_data


def multipart_upload(boto3_client, transfer_config, file_path, bucket_name, object_key):

    try:
        boto3_client.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=object_key,
            Config=transfer_config
        )

        print(f"File '{file_path}' uploaded to '{bucket_name}/{object_key}' using multipart upload.")
    except Exception as e:
        raise e



def convert_timezone(time:datetime, timezone='Asia/Ho_Chi_Minh') -> datetime:
    # Convert the initial datetime object to 'Asia/Ho_Chi_Minh' timezone
    vietnam_timezone = pytz.timezone(timezone)
    converted_datetime = time.astimezone(vietnam_timezone)
    converted_datetime_str = converted_datetime.strftime("%Y-%b-%d_%H:%M:%S")


    return converted_datetime_str


def query_and_log(user, password, account, boto3_client, bucket_name):
    # Set up Snowflake connection parameters
    conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse='MYWH',
    database='MyDB',
    schema='test'
)
    # Create a cursor to execute SQL
    cursor = conn.cursor()

    # Your Snowflake SQL query
    try:
        query = '''
            SELECT * FROM TABLE (INFORMATION_SCHEMA.COPY_HISTORY(
                table_name => 'MyDB.test.terrorism_table',
                START_TIME => DATEADD(MINUTE,-10,CURRENT_TIMESTAMP())))
            ORDER BY LAST_LOAD_TIME DESC
            LIMIT 1;
        '''

        # Execute the query
        cursor.execute(query)

        fields = [x[0] for x in cursor.description]

        # Fetch all results
        row = cursor.fetchall()[0]
        print(row)
        result = {field: value if not isinstance(value, datetime) else convert_timezone(value) for field, value in zip(fields, row)}
        print(result)
        # Convert dictionary to JSON string
        json_data = json.dumps(result)
        
        # Upload JSON data to S3
        boto3_client.put_object(
            Bucket=bucket_name,
            Key=create_file_name('logs/', 'logfile', 'json'),
            Body=json_data
        )

    except Exception as e:
        raise e

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()


