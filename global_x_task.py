import requests
import pandas as pd
import boto3
import configparser
from io import StringIO
from datetime import datetime
import time

class ETFDataProcessor:
    def __init__(self, config_path):
        # Initialize configuration parser
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        
        # Extract configuration values
        self.polygon_api_key = self.config['DEFAULT']['polygon_api_key']
        self.fred_api_key = self.config['DEFAULT']['fred_api_key']
        self.s3_bucket_name = self.config['DEFAULT']['s3_bucket_name']
        self.aws_access_key_id = self.config['DEFAULT']['aws_access_key_id']
        self.aws_secret_access_key = self.config['DEFAULT']['aws_secret_access_key']
        self.aws_region = self.config['DEFAULT'].get('aws_region', 'us-east-1')
        self.athena_s3_staging_dir = self.config['DEFAULT']['athena_s3_staging_dir']
        self.athena_region = self.config['DEFAULT']['athena_region']
        
        # Initialize AWS S3 resource using s3_client
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=self.aws_access_key_id,
                                      aws_secret_access_key=self.aws_secret_access_key,
                                      region_name=self.aws_region)
        
        # Initialize Athena client using athena_client
        self.athena_client = boto3.client('athena',
                                          aws_access_key_id=self.aws_access_key_id,
                                          aws_secret_access_key=self.aws_secret_access_key,
                                          region_name=self.athena_region)
        
        # Dates and ETF Specifications
        self.etf_symbols = ['SPY', 'VOO', 'QQQ']
        self.start_date = '2015-01-01'
        self.end_date = '2024-12-31'
    
    def fetch_etf_data(self, symbol):
        url = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{self.start_date}/{self.end_date}?apiKey={self.polygon_api_key}'
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data['results'])
        df['symbol'] = symbol
        return df
    
    def clean_data(self, df):
        df['date'] = pd.to_datetime(df['t'], unit='ms').dt.date
        df.rename(columns={
            'c': 'close_price',
            't': 'timestamp',
            'v': 'volume',
            'vw': 'volume_weighted_avg_price',
            'o': 'opening_price',
            'h': 'highest_price',
            'l': 'lowest_price',
            'n': 'number_of_trades'
        }, inplace=True)
        return df
    
    def fetch_secondary_data(self):
        url = f'https://api.stlouisfed.org/fred/series/observations?series_id=UNRATE&api_key={self.fred_api_key}&file_type=json'
        response = requests.get(url)
        data = response.json()
        observations = data['observations']
        df = pd.DataFrame(observations)
        df['date'] = pd.to_datetime(df['date']).dt.date
        df.rename(columns={'value': 'unemployment_rate'}, inplace=True)
        df['unemployment_rate'] = df['unemployment_rate'].astype(float)
        return df
    
    def integrate_data(self, etf_data, secondary_data):
        etf_data.reset_index(inplace=True)
        integrated_data = pd.merge(etf_data, secondary_data, on='date', how='left')
        integrated_data.set_index('date', inplace=True)
        return integrated_data
    
    def store_data_in_s3(self, df, filename):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        csv_buffer.seek(0)
        
        try:
            self.s3_client.put_object(Bucket=self.s3_bucket_name, Key=filename, Body=csv_buffer.getvalue())
            print("########################################################################")
            print(f"File '{filename}' uploaded successfully.")
            print("########################################################################")
        except Exception as e:
            print(f"Failed to upload to S3: {e}")
    
    def execute_athena_query(self, query):
        try:
            # Start query execution
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': 'default_globalx',
                    'Catalog': 'AwsDataCatalog'
                },
                ResultConfiguration={
                    'OutputLocation': self.athena_s3_staging_dir,
                    'EncryptionConfiguration': {
                        'EncryptionOption': 'SSE_S3'
                    }
                },
                WorkGroup='primary'
            )
            query_id = response.get('QueryExecutionId')
            print(f"Query started successfully, Query ID: {query_id}")
            
            # Polling Athena for query execution status
            while True:
                status = self.athena_client.get_query_execution(QueryExecutionId=query_id)
                query_status = status['QueryExecution']['Status']['State']
                if query_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(5)  # Wait for 5 seconds before polling again
            
            if query_status == 'SUCCEEDED':
                print("########################################################################")
                print("Query executed successfully.")
                # Fetching the result
                result = self.athena_client.get_query_results(QueryExecutionId=query_id)
                column_info = result['ResultSet']['ResultSetMetadata']['ColumnInfo']
                columns = [col['Name'] for col in column_info]
                rows = []
                for row in result['ResultSet']['Rows'][1:]:
                    rows.append([col['VarCharValue'] if 'VarCharValue' in col else None for col in row['Data']])
                df = pd.DataFrame(rows, columns=columns)
                print(df)
            else:
                print(f"Query execution failed, Status: {query_status}")
                print(f"Query Execution Details: {status}")
        
        except self.athena_client.exceptions.InvalidRequestException as e:
            print(f"Invalid request: {e}")
        except self.athena_client.exceptions.TooManyRequestsException as e:
            print(f"Too many requests: {e}")
        except self.athena_client.exceptions.InternalServerException as e:
            print(f"Internal server error: {e}")
        except Exception as e:
            print(f"Failed to execute query: {e}")
    
    def execute_multiple_athena_queries(self, queries):
        for query in queries:
            if query.strip():
                self.execute_athena_query(query.strip() + ';')
    
    def main(self):
        all_etf_data = pd.DataFrame()
        
        for symbol in self.etf_symbols:
            etf_data = self.fetch_etf_data(symbol)
            etf_data = self.clean_data(etf_data)
            all_etf_data = pd.concat([all_etf_data, etf_data])
        
        secondary_data = self.fetch_secondary_data()
        integrated_data = self.integrate_data(all_etf_data, secondary_data)
        
        self.store_data_in_s3(integrated_data, 'integrated_etf_data/integrated_etf_data.tsv')
        
        drop_DB = """
            DROP TABLE IF EXISTS default_globalx.integrated_etf_data;
            DROP TABLE IF EXISTS default_globalx.fred_unemp;
            DROP TABLE IF EXISTS default_globalx.polygon;
            DROP VIEW IF EXISTS default_globalx.etl_integrated_etf_data;
            DROP VIEW IF EXISTS default_globalx.extra_metrics;
            DROP VIEW IF EXISTS default_globalx.group_by_stock;
            DROP VIEW IF EXISTS default_globalx.group_by_date;
        """
        
        queries = drop_DB.strip().split(';')
        self.execute_multiple_athena_queries(queries)
        
        create_schema = "CREATE DATABASE IF NOT EXISTS default_globalx;"
        self.execute_athena_query(create_schema)
        
        create_DB = """
            CREATE EXTERNAL TABLE default_globalx.integrated_etf_data (
                date STRING,
                index INT,
                volume DOUBLE,
                volume_weighted_avg_price DOUBLE,
                opening_price DOUBLE,
                close_price DOUBLE,
                highest_price DOUBLE,
                lowest_price DOUBLE,
                timestamp BIGINT,
                number_of_trades INT,
                symbol STRING,
                realtime_start STRING,
                realtime_end STRING,
                unemployment_rate DOUBLE
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\n'
            STORED AS TEXTFILE
            LOCATION 's3://global-x-task/integrated_etf_data'
            TBLPROPERTIES ("skip.header.line.count"="1");
        """
        self.execute_athena_query(create_DB)
        
        first_5_rows = "SELECT * FROM default_globalx.integrated_etf_data LIMIT 5;"
        self.execute_athena_query(first_5_rows)

if __name__ == '__main__':
    processor = ETFDataProcessor('config.ini')
    processor.main()
