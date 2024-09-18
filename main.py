import time
import pandas as pd
from coinbase.rest import RESTClient
import datetime
from dateutil.relativedelta import relativedelta
import logging
import json
from tqdm import tqdm  # Progress bar library
import sys

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a handler that writes log messages to stderr using tqdm's write method
class TqdmLoggingHandler(logging.StreamHandler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(stream=sys.stderr)
        self.setLevel(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)  # Use tqdm's write method
            self.flush()
        except Exception:
            self.handleError(record)

# Add the custom handler to the logger
handler = TqdmLoggingHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def load_config(file_path='config.json'):
    """
    Load configuration parameters from a JSON file.

    Parameters:
    - file_path (str): The path to the configuration file.

    Returns:
    - dict: A dictionary containing configuration parameters.
    """
    with open(file_path, 'r') as f:
        return json.load(f)

def get_chunk_end(start, granularity, end_date):
    """
    Calculate the end time for a data chunk based on granularity.

    Parameters:
    - start (datetime.datetime): The start time of the chunk.
    - granularity (str): The granularity label (e.g., 'ONE_DAY').
    - end_date (datetime.datetime): The overall end date for data fetching.

    Returns:
    - datetime.datetime: The end time for the current chunk.
    """
    if granularity == "ONE_MINUTE":
        chunk_end = start + datetime.timedelta(days=1)
    elif granularity == "FIVE_MINUTE":
        chunk_end = start + datetime.timedelta(days=1)
    elif granularity == "FIFTEEN_MINUTE":
        chunk_end = start + datetime.timedelta(days=1)
    elif granularity == "ONE_HOUR":
        chunk_end = start + datetime.timedelta(days=1)
    elif granularity == "SIX_HOUR":
        chunk_end = start + datetime.timedelta(days=5)
    elif granularity == "ONE_DAY":
        chunk_end = start + relativedelta(months=1)
    else:
        chunk_end = start + datetime.timedelta(days=1)
    
    # Ensure we don't go past the end date
    chunk_end = min(chunk_end, end_date + datetime.timedelta(days=1))
    
    # Adjust chunk_end to be one second before midnight
    return chunk_end - datetime.timedelta(seconds=1)

def fetch_candle_data(client, product_id, start, end, granularity, max_retries, retry_delay):
    """
    Fetches candle data from the Coinbase API.

    Parameters:
    - client (RESTClient): The Coinbase API client.
    - product_id (str): The product identifier (e.g., 'BTC-USDC').
    - start (datetime.datetime): The start time for fetching data.
    - end (datetime.datetime): The end time for fetching data.
    - granularity (str): The granularity label.
    - max_retries (int): Maximum number of retries for API requests.
    - retry_delay (int): Delay between retries in milliseconds.

    Returns:
    - list: A list of candle data or an empty list if an error occurs.
    """
    start_unix = int(start.timestamp())
    end_unix = int(end.timestamp())
    
    logger.debug(f"Debug: start_unix = {start_unix}, end_unix = {end_unix}")
    
    attempts = 0
    while attempts < max_retries:
        try:
            response = client.get_public_candles(product_id=product_id, start=start_unix, end=end_unix, granularity=granularity)
            return response.get('candles', [])
        except Exception as ex:
            attempts += 1
            logger.error(f"Error fetching data for {product_id} {granularity} (Attempt {attempts}/{max_retries}): {str(ex)}")
            if attempts < max_retries:
                time.sleep(retry_delay / 1000)  # Convert milliseconds to seconds
            else:
                logger.critical(f"Max retry attempts reached for chunk. Moving to next chunk.")
                return []

def process_candle_data(candles, product_id, granularity, expected_start, expected_end):
    """
    Processes candle data into a Pandas DataFrame.

    Parameters:
    - candles (list): The list of candles fetched from the API.
    - product_id (str): The product identifier.
    - granularity (str): The granularity label.
    - expected_start (datetime.datetime): The expected start time of the data.
    - expected_end (datetime.datetime): The expected end time of the data.

    Returns:
    - pandas.DataFrame: A DataFrame containing the processed data.
    """
    if candles:
        df = pd.DataFrame(candles)
        df.columns = ["StartUnix", "Low", "High", "Open", "Close", "Volume"]
        df['StartUnix'] = pd.to_numeric(df['StartUnix'])
        df['StartDate'] = pd.to_datetime(df['StartUnix'], unit='s', utc=True)
        df['ProductId'] = product_id
        df['Granularity'] = granularity
        
        min_date = df['StartDate'].min()
        max_date = df['StartDate'].max()
        logger.info(f"Received {len(df)} records for {product_id} {granularity} from {min_date} to {max_date}")
        
        # Check if the received data matches the expected range
        if min_date.date() != expected_start.date() or max_date.date() != expected_end.date():
            logger.warning(f"Date range mismatch for {product_id} {granularity}. Expected: {expected_start} to {expected_end}, Got: {min_date} to {max_date}")
        
        return df[['ProductId', 'Granularity', 'StartDate', 'StartUnix', 'Low', 'High', 'Open', 'Close', 'Volume']]
    return pd.DataFrame()

def process_product_granularity(client, product_id, granularity, start_date, end_date, max_retries, retry_delay):
    """
    Fetches and processes candle data for a specific product and granularity.

    Parameters:
    - client (RESTClient): The Coinbase API client.
    - product_id (str): The product identifier (e.g., 'BTC-USDC').
    - granularity (str): The granularity label.
    - start_date (datetime.datetime): The start date for data fetching.
    - end_date (datetime.datetime): The end date for data fetching.
    - max_retries (int): Maximum number of retries for API requests.
    - retry_delay (int): Delay between retries in milliseconds.
    """
    all_data = []
    current_chunk_start = start_date

    # Estimate total number of chunks for the progress bar
    chunk_duration = get_chunk_end(current_chunk_start, granularity, current_chunk_start) - current_chunk_start
    total_chunks = int((end_date - start_date) / chunk_duration) + 1

    with tqdm(total=total_chunks, desc=f"{product_id} {granularity}", unit="chunk") as pbar:
        while current_chunk_start <= end_date:
            chunk_end = get_chunk_end(current_chunk_start, granularity, end_date)

            logger.info(f"Fetching data for {product_id} {granularity} from {current_chunk_start} to {chunk_end}")
            
            candles = fetch_candle_data(client, product_id, current_chunk_start, chunk_end, granularity, max_retries, retry_delay)
            chunk_df = process_candle_data(candles, product_id, granularity, current_chunk_start, chunk_end)
            
            if not chunk_df.empty:
                all_data.append(chunk_df)
            else:
                logger.warning(f"No data returned for chunk {current_chunk_start} to {chunk_end}")
            
            # Set the start of the next chunk to be the end of the current chunk plus one second
            current_chunk_start = chunk_end + datetime.timedelta(seconds=1)

            # Add a small delay to avoid hitting rate limits
            time.sleep(0.5)

            pbar.update(1)  # Update progress bar
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['ProductId', 'Granularity', 'StartUnix']).sort_values('StartDate')
        
        # Filter to include all data points up to and including the end date
        final_df = final_df[final_df['StartDate'] <= end_date]
        
        output_file = f"candle_data_{product_id}_{granularity}.csv"
        final_df.to_csv(output_file, index=False)
        logger.info(f"Candle data exported to {output_file}")
        logger.info(f"Total rows in CSV (including header): {len(final_df) + 1}")
    else:
        logger.warning(f"No data returned for {product_id} {granularity} for the specified time range.")

def main():
    """
    Main function to execute the script.
    """
    config = load_config()
    
    max_retries = config.get('MaxRetryAttempts', 3)
    retry_delay = config.get('RetryDelayMilliseconds', 2000)
    product_ids = config.get('ProductIds', ["BTC-USDC"])
    granularities = config.get('Granularities', ["ONE_DAY"])
    start_date = datetime.datetime.fromisoformat(config.get('StartDate', "2023-01-01")).replace(tzinfo=datetime.timezone.utc)
    end_date = datetime.datetime.fromisoformat(config.get('EndDate', "2023-12-31")).replace(hour=23, minute=59, second=59, tzinfo=datetime.timezone.utc)

    client = RESTClient()

    logger.info(f"CryptoCandleDataToCSV started at: {datetime.datetime.now()}")

    # Loop over products and granularities without progress bars
    for product_id in product_ids:
        logger.info(f"Processing product: {product_id}")
        for granularity in granularities:
            logger.info(f"Processing granularity: {granularity}")
            process_product_granularity(client, product_id, granularity, start_date, end_date, max_retries, retry_delay)

    logger.info(f"Application completed at: {datetime.datetime.now()}")

if __name__ == "__main__":
    main()
