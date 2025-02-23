

#import relevant libraries or modules
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, from_unixtime, regexp_replace, when, unix_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType,DoubleType,LongType, ArrayType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json
from logging_config import setup_logging

from polygon import RESTClient
import time #Need to add delays between API request as there is a limit of 5calls/minute
import config
import json
from typing import cast
from urllib3 import HTTPResponse
import logging
import os


def create_spark_session(app_name="stock_analysis_2023"):
    #Create sparksession
    return SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()

def read_csv_into_df(spark, stock_list):
    #Read csv file
    df_stock_name = spark.read.csv(stock_list, header=True, inferSchema=True)
    
    df_stock_name.write.parquet("stock_names_to_analyse_parquet",mode="overwrite")
    df_read=spark.read.parquet("stock_names_to_analyse_parquet")
    df_read.printSchema()
    df = df_stock_name.withColumn('symbol', 
                    when(col("symbol")==  'FB','META')
                .when(col("symbol")== 'ANTM','ELV')
                                  .otherwise(col('symbol'))
                                         )
    
    #Get the list of ticker symbols from the dataframe.
    symbols_to_analyse = [row['symbol'] for row in df.select("symbol").collect()]
    
    #check we have 100
    print(f"Number of tickers: {len(symbols_to_analyse)}")
    return symbols_to_analyse



def fetch_batch(batch, client, time_frame,start_date, end_date):

    try:
        data=[]
        #Requests data for a ticker one by one.
        for ticker in batch:
            print(f"Fetching data for {ticker}...")
            aggregate = client.get_aggs(
                    ticker,
                    1,  #Aggregation multiplier
                    time_frame, 
                    start_date,
                    end_date,
                    raw = True #Requests raw JSON response
                    )
            #Pase the JSON response
            response_data = json.loads(aggregate.data)

            
            if response_data:
                #appends the relevant data we need to a list
                data.append({
                    'ticker':ticker,
                    'results':response_data['results']
                    })
            else:
                print(f"No results found for {ticker}")
        return data
    #Error handling
    except Exception as e:
        logger.error(f"Error fetching data for {batch}: {e}")
        return []


def fetch_batch_with_rety(batch, client, time_frame,start_date, end_date,retries=2,delay=60.1):
#Retries API call incase of failure due to network issues. 
    for attempt in range(retries):
        try:
            return fetch_batch(batch, client, time_frame,start_date, end_date)
        except Exception as e:
            logger.error(f"Error fetching data for batch {batch}:{e}")
            if attempt<retries-1:
                logger.error(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"Failed after {retries} attempts.")
                return []

def flat_map(entry):
    try:
        #ensure ticker exists and is not a string
        ticker = entry['ticker']
        if not isinstance(ticker,str):
            raise TypeError(f"Expected 'ticker' to be string, got {type(ticker)}")
        #Process the data field
        if 'data' not in entry or not isinstance(entry['data'],list):
            raise ValueError(f"Expected 'data' to be a list but got {type(entry.get('data'))}")
        
        result = []
        for item in entry['data']:
            c = float(item.get("c",0))
            t = item.get('t',0)


            if not isinstance(t,int) or t<0:
                raise ValueError(f"Invalid timestamp value for ticker {ticker}:{t}. Expected a non-negative integer.")

            result.append((ticker,c,t))
        logging.info(f"Sucessfully processed ticker: {ticker} with {len(entry['data'])} records.")
        return result

    except KeyError as e:
        logging.error(f"Missing key in entry: {entry}, KeyError: {e}")
    except ValueError as e:
        logging.error(f"Value Error in entry: {entry}, ValueError: {e}")
    except TypeError as e:
        logging.error(f"Type Error in entry: {entry}, TypeError: {e}")
    except Exception as e:
        logging.critical(f"Unexpected Error in processing: {entry}, Error as {e}", exc_info=True)
    #Return empty list if there is an error    
    return []


def process_all_tickers_and_write(symbols_to_analyse, client, batch_size, writer, time_frame, start_date, end_date, spark):
    #Processes all tickers by fetching it in batches
    #I'll ge the list to hold a 1000records then I do a write to disk and cleam memory.
    write_batch_size=1000
    accumulated_result = []
    for i in range(0,len(symbols_to_analyse), batch_size):
        #Iterate through tickers in batches of 5.
        batch=symbols_to_analyse[i:i+batch_size]
        #Call above function on each batch
        batch_data = fetch_batch_with_rety(batch, client, time_frame,start_date, end_date)
        

        #Ticker data should in a dictionary format
        for ticker_data in batch_data:
            if not isinstance(ticker_data,dict):
                print(f"Unexpected data format: {ticker_data}")
                continue
            
            #Get the ticker symbol and the associated data as per API definition
            ticker=ticker_data.get("ticker", "")
            results=ticker_data.get("results",[])
            #Create a record containing the ticker symbol and its associated results.
            #Organises data + maintains a consistent format. 
            record={
                "ticker":ticker,
                "data":results,
            }

            
            flattened_result=flat_map(record)
            if flattened_result:
                accumulated_result.extend(flattened_result)

            #write to disk if list exceeds threshold, this prevent OOM errors. 
            if len(accumulated_result)>=write_batch_size:
                writer.write_to_parquet(accumulated_result,spark)
                accumulated_result.clear()

        if accumulated_result:
            writer.write_to_parquet(accumulated_result,spark)

        print("Processing of a batch complete")
        
        print("Waiting 60.1secs")
        time.sleep(60.1) #Sleep for 1 minute

    return

class DynamicWriter():
    "Handles writing data to parquet dynamically everytime the code is run. Default variable set as true for is_first_write."
    def __init__(self, file_path, spark):
        self.file_path=file_path
        self.is_first_write=True
        self.spark=spark

    
    def write_to_parquet(self, flattened_result, spark):
        #Define schema of the dataframe.
        schema=StructType(
            [
                StructField("ticker",StringType(),True),
                StructField("c",DoubleType(),True),
                StructField("t",LongType(),True)
            ]
        )


        write_mode="overwrite" if self.is_first_write else "append"
        spark_df = self.spark.createDataFrame(flattened_result,schema)
        spark_df.write.mode(write_mode).parquet(self.file_path)
        self.is_first_write=False
        print(f"Data written with {write_mode}")

def main():
    print(os.getcwd())
    setup_logging()
    logger = logging.getLogger(__name__)
    logging.info("Logging is now working in the raw_layer script.")
        
    spark = create_spark_session()
    symbols_to_analyse=read_csv_into_df(spark, stock_list="stocks.csv")
    
    #Set the constants we need for the API
    time_frame='day'
    start_date= '2024-01-01'
    end_date = '2024-12-31'
    batch_size=5
    #Initialise the API client
    client = RESTClient(config.API_KEY)
    

    file_path="output_file.parquet"
    writer=DynamicWriter(file_path,spark)
    process_all_tickers_and_write(symbols_to_analyse, client, batch_size, writer, time_frame, start_date, end_date, spark)

    

if __name__=="__main__":
    main()
















