#!/usr/bin/env python3.10
"""
Download Historical & Update the latest Stock Daily Data,
Store Local Parquet (one file), breakdown to JSON files by ticker in both local folder and S3 bucket.
API created under chalice_taiapi
"""

import asyncio
import pandas as pd
import polars as pl
import time
import json
import os
import math
import logging
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple, Dict, Any

from TAI.source import alpaca
from TAI.data import DataMaster

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

# Set global variables directly in the code
LOOKBACK_YEARS = 20  # Number of years to look back
PRINT_LOG = False     # Enable detailed logging
MAX_CONCURRENT_REQUESTS = 5  # Max concurrent requests
BATCH_SIZE = 50      # Batch size for data fetching
PARQUET_FILE = 'stock_daily_ohlc.parquet'  # Parquet file name
JSON_DIR = 'data/stock_daily_bar'  # Directory to store JSON files

if PRINT_LOG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

dm = DataMaster()

def get_lookback_period_in_days() -> Tuple[int, datetime]:
    """Calculate the number of days to look back from the start date."""
    today = date.today()
    start_date = today - timedelta(days=LOOKBACK_YEARS * 365)
    lookback_period = (today - start_date).days
    return lookback_period, datetime.combine(start_date, datetime.min.time())

LOOKBACK_PERIOD_DAYS, START_DATE = get_lookback_period_in_days()

def get_sp500_symbols() -> List[str]:
    """Fetches the list of S&P 500 company symbols from Wikipedia or from local CSV if the site is down."""
    csv_file = 'sp500_symbols.csv'
    try:
        # Check if CSV is recent (e.g., within the last week)
        if os.path.exists(csv_file):
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(csv_file))
            if datetime.now() - file_mod_time < timedelta(days=7):
                sp500_df = pd.read_csv(csv_file)
                symbols = sp500_df['Symbol'].tolist()
                symbols = [symbol.replace('.', '-') for symbol in symbols]
                logger.info(f"Loaded S&P 500 symbols from {csv_file}")
                return symbols
        # Fetch from Wikipedia
        sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        sp500_tables = pd.read_html(sp500_url)
        sp500_df = sp500_tables[0]
        symbols = sp500_df['Symbol'].tolist()
        symbols = [symbol.replace('.', '-') for symbol in symbols]
        # Save to CSV
        sp500_df.to_csv(csv_file, index=False)
        logger.info(f"S&P 500 symbols fetched from Wikipedia and saved to {csv_file}")
        return symbols
    except Exception as e:
        logger.error(f"Error fetching S&P 500 symbols from Wikipedia: {e}")
        # Try to load from CSV
        if os.path.exists(csv_file):
            sp500_df = pd.read_csv(csv_file)
            symbols = sp500_df['Symbol'].tolist()
            symbols = [symbol.replace('.', '-') for symbol in symbols]
            logger.info(f"Loaded S&P 500 symbols from {csv_file}")
            return symbols
        else:
            logger.error(f"No local CSV file {csv_file} found.")
            return []

def get_nasdaq100_symbols() -> List[str]:
    """Fetch NASDAQ-100 symbols from Wikipedia or from local CSV if the site is down."""
    csv_file = 'nasdaq100_symbols.csv'
    try:
        # Check if CSV is recent (e.g., within the last week)
        if os.path.exists(csv_file):
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(csv_file))
            if datetime.now() - file_mod_time < timedelta(days=7):
                nasdaq100_df = pd.read_csv(csv_file)
                symbols = nasdaq100_df['Ticker'].tolist()
                symbols = [symbol.replace('.', '-') for symbol in symbols]
                logger.info(f"Loaded NASDAQ-100 symbols from {csv_file}")
                return symbols
        # Fetch from Wikipedia
        url = 'https://en.wikipedia.org/wiki/NASDAQ-100'
        tables = pd.read_html(url)
        nasdaq100_df = tables[4]  # Adjust the index if necessary
        symbols = nasdaq100_df['Ticker'].tolist()
        symbols = [symbol.replace('.', '-') for symbol in symbols]
        # Save to CSV
        nasdaq100_df.to_csv(csv_file, index=False)
        logger.info(f"NASDAQ-100 symbols fetched from Wikipedia and saved to {csv_file}")
        return symbols
    except Exception as e:
        logger.error(f"Error fetching NASDAQ-100 symbols from Wikipedia: {e}")
        # Try to load from CSV
        if os.path.exists(csv_file):
            nasdaq100_df = pd.read_csv(csv_file)
            symbols = nasdaq100_df['Ticker'].tolist()
            symbols = [symbol.replace('.', '-') for symbol in symbols]
            logger.info(f"Loaded NASDAQ-100 symbols from {csv_file}")
            return symbols
        else:
            logger.error(f"No local CSV file {csv_file} found.")
            return []

def get_popular_stock_symbols() -> List[str]:
    """Returns a list of commonly traded stocks outside the S&P 500."""
    popular_stocks = [
        # International Stocks (ADRs)
        'BABA', 'TCEHY', 'NIO', 'JD', 'PDD', 'RIO', 'SONY', 'BP', 'TOT', 'VWAGY', 'UBS', 'CS', 'SAN',
        # Technology Stocks
        'ZM', 'NET', 'SHOP', 'SNOW', 'CRWD', 'PLTR', 'U', 'OKTA', 'TEAM', 'DDOG', 'ZS', 'DOCU', 'FSLY',
        # Biotech and Pharmaceutical Stocks
        'MRNA', 'NVAX', 'BNTX', 'SAVA', 'IBIO', 'INO',
        # Electric Vehicle Stocks
        'XPEV', 'LI', 'NIO', 'RIVN', 'LCID', 'NKLA',
        # Cryptocurrency and Blockchain-related Stocks
        'COIN', 'MARA', 'RIOT', 'SI', 'BTBT', 'MSTR',
        # Travel and Leisure Stocks
        'ABNB', 'UBER', 'LYFT', 'SPCE', 'RBLX',
        # Social Media and Internet Stocks
        'SNAP', 'PINS', 'TWTR',
        # Renewable Energy Stocks
        'RUN', 'SEDG', 'ENPH', 'PLUG', 'BE', 'BLDP',
        # Other Popular Stocks
        'GME', 'AMC', 'BB', 'NOK', 'FUBO', 'OPEN', 'DKNG', 'HOOD', 'SOFI', 'AFRM', 'ROKU', 'ETSY', 'SQ', 'PYPL',
    ]
    return popular_stocks

def get_common_etf_symbols() -> List[str]:
    """Returns a comprehensive list of commonly traded ETFs."""
    etf_symbols = [
        # Broad Market ETFs
        'SPY', 'IVV', 'VOO', 'VTI', 'ITOT', 'SCHB',
        # Sector ETFs
        'XLK', 'XLF', 'XLV', 'XLY', 'XLP', 'XLE', 'XLI', 'XLU', 'XLB', 'XLRE', 'XLC',
        # International ETFs
        'EFA', 'IEFA', 'VEA', 'VXUS', 'IXUS', 'VWO', 'IEMG', 'EMXC',
        # Bond ETFs
        'BND', 'AGG', 'LQD', 'HYG', 'JNK', 'BNDX', 'TIP', 'TLT', 'IEF', 'SHY',
        # Commodity ETFs
        'GLD', 'IAU', 'SLV', 'USO', 'UNG', 'DBC',
        # Real Estate ETFs
        'VNQ', 'IYR', 'SCHH', 'XLRE', 'RWR',
        # Dividend ETFs
        'VYM', 'DVY', 'SDY', 'SCHD', 'VIG',
        # Small-Cap ETFs
        'IWM', 'VB', 'IJR', 'SCHA',
        # Mid-Cap ETFs
        'IJH', 'VO', 'IWR',
        # Growth and Value ETFs
        'IWF', 'IWD', 'VUG', 'VTV', 'SCHG', 'SCHV',
        # Currency ETFs
        'UUP', 'FXE', 'FXY', 'FXB',
        # Inverse and Leveraged ETFs (use with caution)
        'SDS', 'QID', 'TZA', 'SSO', 'QLD', 'UCO',
        # Thematic ETFs
        'ARKK', 'ARKG', 'ARKW', 'BOTZ', 'KWEB', 'TAN',
        # Other Popular ETFs
        'QQQ', 'DIA', 'MDY', 'EEM', 'EWJ', 'EWZ', 'GDX', 'EWT', 'VT',
        # Emerging Markets ETFs
        'VWO', 'EEM', 'SCHE', 'IEMG',
        # Additional ETFs
        'VTIP', 'MUB', 'EMB', 'BKLN', 'FLOT', 'SRLN', 'BIV', 'BSV', 'BSCJ',
        'BSCK', 'BSCL', 'BSJM', 'BSJL', 'BSJK', 'BNDW', 'SPDW', 'SPEM',
        'SCHF', 'SCHZ', 'SCHP', 'SCHA', 'SCHG', 'SCHO', 'SCHR', 'SCHI',
        'SHE', 'NOBL', 'MTUM', 'QUAL', 'VLUE', 'USMV', 'HDV', 'IUSV',
        'IUSG', 'IUSB', 'IAGG', 'IEI', 'IWM', 'IJH', 'IWN', 'IWO', 'IWP',
        'IWS', 'IWL', 'ACWI', 'ACWX', 'AAXJ', 'QAI', 'SDY', 'DIA',
        'EWG', 'EWH', 'EWU', 'EWT', 'EWW', 'EWY', 'EWA', 'EWO', 'EWL',
        'EWD', 'EWP', 'EWN', 'EWS', 'EWK', 'EWM', 'EPOL', 'EPI', 'PIN',
        'RSX', 'FXI', 'ASHR', 'MCHI', 'INDA', 'THD', 'PLND', 'TUR', 'GREK',
        'NORW', 'EIS', 'ENZL', 'ARGT', 'EZA', 'EWZ', 'EWJ', 'EWY', 'EWM',
        'EIDO', 'EPU', 'EPHE', 'ECH', 'GXC', 'VNM', 'FM', 'KSA',
        'MEXX', 'JPNL', 'JPN', 'JOF', 'AXJL', 'AIA', 'IPAC', 'EEMA', 'FNI'
    ]
    return etf_symbols

# Fetch symbols
sp500_symbols = get_sp500_symbols()
nasdaq100_symbols = get_nasdaq100_symbols()
etf_symbols = get_common_etf_symbols()
popular_stocks = get_popular_stock_symbols()

# Combine symbols and remove duplicates
symbols = list(set(sp500_symbols + nasdaq100_symbols + etf_symbols + popular_stocks))

# Initialize Alpaca class instance
alpaca_handler = alpaca.Alpaca()

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the DataFrame by handling NaNs and invalid data."""
    df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])
    df = df[df['volume'] > 0]  # Exclude entries with zero volume
    return df

async def fetch_stock_data(semaphore: asyncio.Semaphore, symbol: str, start_date: datetime) -> Tuple[str, pd.DataFrame]:
    """Asynchronously fetch historical stock data with exponential backoff."""
    async with semaphore:
        max_retries = 5
        retry_delay = 1  # Start with 1 second
        for attempt in range(max_retries):
            try:
                # Fetch historical data
                if PRINT_LOG:
                    logger.debug(f"Fetching data for {symbol} starting from {start_date.date()}...")
                data = alpaca_handler.get_stock_historical(
                    symbol_or_symbols=symbol,
                    lookback_period=LOOKBACK_PERIOD_DAYS,
                    end=datetime.now(),
                    timeframe='Day',
                    ohlc=True
                )
                if data.empty:
                    raise ValueError(f"No data returned for {symbol}")
                data = clean_data(data)
                return symbol, data
            except Exception as e:
                if 'rate limit' in str(e).lower() or 'too many requests' in str(e).lower():
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    logger.error(f"Error fetching data for {symbol}: {e}")
                    return symbol, None
        # After max retries
        logger.error(f"Max retries reached for {symbol}")
        return symbol, None

async def fetch_latest_data_in_batches(symbols: List[str], existing_data: pd.DataFrame, batch_size: int) -> Tuple[List[pd.DataFrame], List[str]]:
    """Fetch latest data for symbols asynchronously in batches."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    total_symbols = len(symbols)
    num_batches = math.ceil(total_symbols / batch_size)
    all_successful_results = []
    all_failed_symbols = []

    for i in range(num_batches):
        batch_symbols = symbols[i*batch_size : (i+1)*batch_size]
        tasks = []

        for symbol in batch_symbols:
            # Determine the start date for each symbol
            if not existing_data.empty and 'symbol' in existing_data.columns:
                if symbol in existing_data['symbol'].unique():
                    last_date = existing_data[existing_data['symbol'] == symbol]['timestamp'].max()
                    start_date = pd.to_datetime(last_date) + timedelta(days=1)
                else:
                    # If the symbol is not in existing data, fetch data from the default start date
                    start_date = START_DATE
            else:
                # existing_data is empty or does not have 'symbol' column
                start_date = START_DATE

            # Skip if start_date is after today
            if start_date.date() > date.today():
                if PRINT_LOG:
                    logger.debug(f"No new data for {symbol}.")
                continue

            tasks.append(fetch_stock_data(semaphore, symbol, start_date))

        results = await asyncio.gather(*tasks)
        successful_results = []
        failed_symbols = []

        for symbol, data in results:
            if data is not None and not data.empty:
                successful_results.append(data)
            elif data is None:
                failed_symbols.append(symbol)
            # No need to add symbols with empty data (no new data)

        all_successful_results.extend(successful_results)
        all_failed_symbols.extend(failed_symbols)

    return all_successful_results, all_failed_symbols

def update_parquet_with_latest_data():
    """Update the Parquet file with the latest data for all tickers."""
    # Record the start time
    start_time = time.time()

    # Check if Parquet file exists
    if os.path.exists(os.path.join('data', PARQUET_FILE)):
        # Read existing data using Polars
        existing_data = load_data_from_parquet(os.path.join('data', PARQUET_FILE))
    else:
        # If file doesn't exist, create an empty DataFrame
        existing_data = pd.DataFrame()

    # Run async fetch for latest data in batches
    loop = asyncio.get_event_loop()
    successful_results, failed_symbols = loop.run_until_complete(
        fetch_latest_data_in_batches(symbols, existing_data, batch_size=BATCH_SIZE)
    )

    if successful_results:
        # Combine new data
        new_data = pd.concat(successful_results, ignore_index=True)
        # Combine with existing data
        combined_df = pd.concat([existing_data, new_data], ignore_index=True)
        # Remove duplicates
        combined_df.drop_duplicates(subset=['symbol', 'timestamp'], inplace=True)
        # Sort the data
        combined_df.sort_values(by=['symbol', 'timestamp'], inplace=True)
        # Store the data in Parquet
        store_data_in_parquet(combined_df, PARQUET_FILE)
    else:
        logger.info("No new data was fetched.")

    # Print failed symbols if there were any errors
    if failed_symbols:
        logger.error(f"Failed to fetch data for the following symbols: {failed_symbols}")

    # Record the end time
    end_time = time.time()
    total_time = end_time - start_time
    logger.info(f"Update completed in {total_time:.2f} seconds")

def store_data_in_parquet(df: pd.DataFrame, file_name: str):
    """Store the DataFrame in Parquet format using Polars with compression."""
    if df.empty:
        logger.info("No data to store.")
        return

    try:
        dm.create_dir()
        pl_df = pl.from_pandas(df)
        pl_df.write_parquet(os.path.join('data', file_name), compression='snappy')
        if PRINT_LOG:
            logger.debug(f"Data successfully stored in {file_name} using Polars with compression")
    except Exception as e:
        logger.error(f"Error storing data to Parquet: {e}")

def load_data_from_parquet(file_path: str) -> pd.DataFrame:
    """Load data from Parquet file using Polars."""
    if os.path.exists(file_path):
        pl_df = pl.read_parquet(file_path)
        df = pl_df.to_pandas()
        return df
    else:
        return pd.DataFrame()

def save_group_to_json(group: pd.DataFrame, symbol: str) -> str:
    """Save grouped data to JSON without compression."""
    group = group.copy()
    group['date'] = group['timestamp'].dt.strftime('%Y-%m-%d')
    group.drop(columns=['timestamp'], inplace=True)
    json_records = group.to_dict(orient='records')
    json_file_name = f'{symbol}.json'
    local_file_path = os.path.join(JSON_DIR, json_file_name)
    os.makedirs(JSON_DIR, exist_ok=True)
    with open(local_file_path, 'w', encoding='utf-8') as f:
        json.dump(json_records, f)
    return local_file_path

def process_and_save_symbol(grouped_data: Tuple[str, pd.DataFrame]):
    """Process and save data for a single symbol."""
    symbol, group = grouped_data
    local_file_path = save_group_to_json(group, symbol)
    # Load JSON data to pass to dm.save_s3
    with open(local_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    dm.save_s3(
        json_data,
        'jtrade1-dir',
        'api/stock_daily_bar',
        symbol + '.json',
        use_polars=False,
        delete_local=True
    )
    if PRINT_LOG:
        logger.debug(f"Processed and saved data for {symbol}")

def main():
    print('PROCESS STARTS AT : {}'.format(datetime.now()))
    logger.info('PROCESS STARTS')
    update_parquet_with_latest_data()  # Update the Parquet file with the latest data

    updated_df = load_data_from_parquet(os.path.join('data', PARQUET_FILE))

    # Group the data by symbol
    grouped = updated_df.groupby('symbol')

    # Process and save data in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_and_save_symbol, grouped)

    logger.info('PROCESS ENDS')
    print('PROCESS ENDS AT : {}'.format(datetime.now()))

if __name__ == "__main__":
    main()
