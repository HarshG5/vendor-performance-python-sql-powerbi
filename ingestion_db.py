'''import necessary libraries'''
import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

'''logging configuration'''
logging.basicConfig(
    filename="logs/.log",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
)

'''create_engine to connect to the database'''
engine = create_engine('sqlite:///vendor_performance.db')

'''function to ingest data into the database'''
def ingest_db(data,table_name,engine):
        data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f'Table {table_name} ingested successfully.')

'''load data from csv files and ingest into database'''
def load_data():
    start_time = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            vendor_df = pd.read_csv('data/' + file)
            table_name = file.replace('.csv','')
            ingest_db(vendor_df, table_name, engine)
            logging.info(f'Ingested {file} into table {table_name}')
            end_time = time.time()
            logging.info(f'Time taken to ingest {file}: {end_time - start_time}/60 minutes')
    Total_time = time.time() - start_time
    logging.info('All files ingested successfully.')
    logging.info(f'Total time taken to ingest all files: {Total_time}/60 minutes')
    

if __name__ == "__main__":
     load_data()