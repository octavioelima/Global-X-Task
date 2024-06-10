import pandas as pd
import boto3
import configparser
import matplotlib.pyplot as plt
import seaborn as sns

# Completely disable scientific notation in pandas
pd.options.display.float_format = '{:.2f}'.format

class AthenaDataVisualizer:
    def __init__(self, config_path):
        # Initialize configuration parser
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        
        # Extract configuration values
        self.athena_s3_staging_dir = self.config['DEFAULT']['athena_s3_staging_dir']
        self.athena_region = self.config['DEFAULT']['athena_region']
        
        # Initialize Athena client using athena_client
        self.athena_client = boto3.client('athena',
                                          aws_access_key_id=self.config['DEFAULT']['aws_access_key_id'],
                                          aws_secret_access_key=self.config['DEFAULT']['aws_secret_access_key'],
                                          region_name=self.athena_region)
    
    def execute_athena_query(self, query):
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
        return response['QueryExecutionId']
    
    def check_query_status(self, query_id):
        status = self.athena_client.get_query_execution(QueryExecutionId=query_id)
        return status['QueryExecution']['Status']['State']
    
    def fetch_query_results(self, query_id):
        results = self.athena_client.get_query_results(QueryExecutionId=query_id)
        column_info = results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        columns = [col['Name'] for col in column_info]
        rows = []
        for row in results['ResultSet']['Rows'][1:]:
            rows.append([col.get('VarCharValue', None) for col in row['Data']])
        return pd.DataFrame(rows, columns=columns)
    
    def get_extra_metrics_data(self):
        query = "SELECT * FROM extra_metrics;"
        query_id = self.execute_athena_query(query)
        while self.check_query_status(query_id) not in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            pass  # Implement a better wait or timeout logic in production
        return self.fetch_query_results(query_id)
    
    def plot_time_series(self, df, title, x, y, hue=None):
        plt.figure(figsize=(15, 6))
        sns.lineplot(data=df, x=x, y=y, hue=hue)
        plt.title(title)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
    
    def plot_bar_chart(self, df, title, x, y, hue=None):
        plt.figure(figsize=(15, 6))
        sns.barplot(data=df, x=x, y=y, hue=hue)
        plt.title(title)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
    
    def visualize_data(self):
        df = self.get_extra_metrics_data()
        df['date'] = pd.to_datetime(df['date'])
        df[['volume', 'volume_weighted_avg_price', 'opening_price', 'close_price', 'highest_price', 'lowest_price',
             'number_of_trades', 'daily_return', 'moving_avg_50', 'price_change', 'intraday_volatility']] = df[['volume', 
             'volume_weighted_avg_price', 'opening_price', 'close_price', 'highest_price', 'lowest_price', 
             'number_of_trades', 'daily_return', 'moving_avg_50', 'price_change', 'intraday_volatility']].apply(pd.to_numeric)
        
        # Time series plots
        self.plot_time_series(df, 'Average Daily Return by Symbol', 'date', 'daily_return', 'symbol')
        self.plot_time_series(df, 'Intraday Volatility', 'date', 'intraday_volatility', 'symbol')
        self.plot_time_series(df, 'Number of Trades', 'date', 'number_of_trades', 'symbol')
        
        # Bar charts
        self.plot_bar_chart(df.groupby('symbol').mean().reset_index(), 'Average Volume by Symbol', 'symbol', 'volume')
        self.plot_bar_chart(df.groupby('symbol').mean().reset_index(), 'Average Number of Trades by Symbol', 'symbol', 'number_of_trades')

if __name__ == '__main__':
    visualizer = AthenaDataVisualizer('../config.ini')
    visualizer.visualize_data()
