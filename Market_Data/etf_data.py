#!/usr/bin/env python3
"""
Historical ETF Data Downloader
Downloads OHLC data for Indian ETFs from Yahoo Finance and stores in PostgreSQL database.
"""

import psycopg2
from psycopg2.extras import execute_batch
import yfinance as yf
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
import sys
from typing import List, Dict, Optional
import os
import pytz

# IST timezone
IST = pytz.timezone('Asia/Kolkata')

# Database connection string
DATABASE_URL = "postgresql://neondb_owner:npg_WgVhOYtnP12l@ep-solitary-silence-a1yoj91r.ap-southeast-1.aws.neon.tech/MarketData?sslmode=require&channel_binding=require"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_download.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ETFDataDownloader:
    def __init__(self):
        self.start_date = "2010-01-01"  # Keep for initial historical loads
        # Default end_date to today in IST timezone
        self.end_date = datetime.now(IST).strftime("%Y-%m-%d")
        self.delay_between_requests = 1.5  # seconds
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        # Initialize database
        self.init_database()
        
        # ETF symbols list (only ETFs)
        self.etf_symbols = [
            "TATAGOLD",
            "GOLDBEES",
            "SILVERBEES",
            "TATSILV",
            "METALIETF",
            "ITBEES",
            "GOLDCASE",
            "SILVERCASE",
            "LIQUIDCASE",
            "GOLDIETF",
            "SILVERIETF",
            "SETFGOLD",
            "HDFCGOLD",
            "HDFCSILVER",
            "NIFTYBEES",
            "PSUBNKBEES",
            "PHARMABEES",
            "METAL",
            "ALPL30IETF",
            "LIQUIDBEES",
            "OILIETF",
            "SBISILVER",
            "SILVER",
            "MIDCAPETF",
            "PVTBANIETF",
            "LTGILTBEES",
            "GOLD1",
            "LOWVOLIETF",
            "SILVERETF",
            "ALPHA",
            "MODEFENCE",
            "AXISGOLD",
            "MID150CASE",
            "GOLDETF",
            "SMALLCAP",
            "SILVER1",
            "MOREALTY",
            "MON100",
            "MOM30IETF",
            "MIDCAPIETF",
            "BSLGOLDETF",
            "FMCGIETF",
            "GOLDSHARE",
            "CPSEETF",
            "MOSMALL250",
            "SETFNIF50",
            "SILVERADD",
            "MOGOLD",
            "AXISILVER",
            "ITIETF",
            "GROWWSLVR",
            "AONEGOLD",
            "TOP100CASE",
            "HDFCSML250",
            "MOCAPITAL",
            "MID150BEES",
            "BANKBEES",
            "LIQUIDADD",
            "GROWWGOLD",
            "FINIETF",
            "NV20IETF",
            "NIFTYIETF",
            "GOLDETFADD",
            "SILVERAG",
            "MAHKTECH",
            "NEXT50IETF",
            "AUTOIETF",
            "AONETOTAL",
            "MAFANG",
            "BANKIETF",
            "PSUBNKIETF",
            "ALPHAETF",
            "MOMENTUM50",
            "MOM100",
            "LIQUID1",
            "GROWWPOWER",
            "LIQUIDBETF",
            "ICICIB22",
            "LIQUIDIETF",
            "ABSLPSE",
            "ESILVER",
            "HDFCMID150",
            "IT",
            "GROWWRAIL",
            "ITETF",
            "CONSUMER",
            "MULTICAP",
            "MONIFTY500",
            "MOSILVER",
            "TOP10ADD",
            "AONENIFTY",
            "UTIBANKETF",
            "GROWWLIQID",
            "MONQ50",
            "AXISBPSETF",
            "COMMOIETF",
            "MASPTOP50",
            "FLEXIADD",
            "GROWWSC250",
            "CASHIETF",
            "HDFCNIFBAN",
            "GROWWDEFNC",
            "JUNIORBEES",
            "GROWWNET",
            "SML100CASE",
            "BSLNIFTY",
            "NV20",
            "EGOLD",
            "QGOLDHALF",
            "GROWWEV",
            "UTINIFTETF",
            "MIDSELIETF",
            "NIFTYCASE",
            "AUTOBEES",
            "BFSI",
            "EVIETF",
            "GILT5YBEES",
            "MIDSMALL",
            "INFRAIETF",
            "GROWWMOM50",
            "GROWWNIFTY",
            "HDFCMOMENT",
            "TECH",
            "EVINDIA",
            "VAL30IETF",
            "LTGILTCASE",
            "HDFCNIFTY",
            "HEALTHY",
            "UTINEXT50",
            "TOP15IETF",
            "LIQUID",
            "BSE500IETF",
            "NIF100IETF",
            "CONSUMBEES",
            "NIFTYETF",
            "PSUBANKADD",
            "NIFTY1",
            "NV20BEES",
            "ELM250",
            "MOMENTUM",
            "DIVOPPBEES",
            "QUAL30IETF",
            "LOWVOL1",
            "HDFCPVTBAN",
            "SBIBPB",
            "GROWWRLTY",
            "BBNPPGOLD",
            "GROWWN200",
            "PSUBANK",
            "MOMOMENTUM",
            "ECAPINSURE",
            "LIQUIDETF",
            "MNC",
            "MOVALUE",
            "MOENERGY",
            "GOLD360",
            "SILVER360",
            "HNGSNGBEES",
            "HDFCNIFIT",
            "HEALTHIETF",
            "INTERNET",
            "MOMIDMTM",
            "LICNETFGSC",
            "BANKPSU",
            "NIFTYQLITY",
            "ABSLBANETF",
            "LIQUIDPLUS",
            "HDFCNEXT50",
            "HDFCPSUBK",
            "NIF100BEES",
            "MOHEALTH",
            "HDFCNIF100",
            "BANKNIFTY1",
            "MIDCAP",
            "ESG",
            "SETFNIFBK",
            "TNIDETF",
            "GROWWLOVOL",
            "EMULTIMQ",
            "AXISNIFTY",
            "ITETFADD",
            "MOLOWVOL",
            "UNIONGOLD",
            "LIQGRWBEES",
            "NEXT30ADD",
            "MOGSEC",
            "AXISVALUE",
            "CONS",
            "AONELIQUID",
            "HDFCQUAL",
            "GSEC10YEAR",
            "HDFCSENSEX",
            "HDFCGROWTH",
            "SETFNN50",
            "SBINEQWETF",
            "MAKEINDIA",
            "PVTBANKADD",
            "MID150",
            "SETF10GILT",
            "NIFTY100EW",
            "MSCIINDIA",
            "HDFCLOWVOL",
            "CONSUMIETF",
            "HDFCLIQUID",
            "HDFCBSE500",
            "BANKBETF",
            "SENSEXETF",
            "MONIFTY100",
            "EQUAL200",
            "UTISENSETF",
            "MOMENTUM30",
            "NIFTY50ADD",
            "EBBETF0431",
            "NEXT50",
            "GSEC10ABSL",
            "SELECTIPO",
            "HDFCVALUE",
            "EQUAL50ADD",
            "SENSEXIETF",
            "SBIETFIT",
            "MOALPHA50",
            "INFRABEES",
            "GSEC5IETF",
            "EBBETF0433",
            "LICNMID100",
            "ABSLNN50ET",
            "SHARIABEES",
            "BANKETF"
        ]
        
        # Remove any stock symbols from ETF list to ensure no overlap
        # Get stock symbols from stock_data.py to filter them out
        try:
            stock_data_path = os.path.join(os.path.dirname(__file__), 'stock_data.py')
            if os.path.exists(stock_data_path):
                with open(stock_data_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Extract stock_symbols list using regex
                    import re
                    match = re.search(r'self\.stock_symbols\s*=\s*\[(.*?)\]', content, re.DOTALL)
                    if match:
                        stock_list_content = match.group(1)
                        # Extract all quoted strings (stock symbols)
                        stock_symbols = re.findall(r'"([^"]+)"', stock_list_content)
                        stock_symbols_set = set(stock_symbols)
                        
                        # Filter ETF list to remove any stock symbols
                        original_etf_symbols = self.etf_symbols.copy()
                        self.etf_symbols = [s for s in self.etf_symbols if s not in stock_symbols_set]
                        removed_symbols = [s for s in original_etf_symbols if s in stock_symbols_set]
                        removed_count = len(removed_symbols)
                        
                        if removed_count > 0:
                            logger.warning(f"Removed {removed_count} stock symbol(s) from ETF list: {removed_symbols}")
                        else:
                            logger.debug("No stock symbols found in ETF list - list is clean")
        except Exception as e:
            logger.warning(f"Could not filter stock symbols from ETF list: {e}. Proceeding with original ETF list.")
        
        # Use only ETF symbols (after filtering out stock symbols)
        self.all_symbols = self.etf_symbols

    def get_connection(self):
        """Create and return a PostgreSQL database connection using DATABASE_URL."""
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL database: {str(e)}")
            raise

    def init_database(self):
        """Initialize PostgreSQL database with proper schema."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Create etf_info table (for ETFs)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS etf_info (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT UNIQUE NOT NULL,
                    name TEXT,
                    type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create etf_data table (for ETF data)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS etf_data (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    adjusted_close REAL,
                    volume BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, date),
                    FOREIGN KEY (symbol) REFERENCES etf_info (symbol)
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_date ON etf_data (symbol, date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON etf_data (symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON etf_data (date)')
            
            conn.commit()
            logger.info("PostgreSQL database initialized: MarketData (Neon Cloud)")
            
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def get_yahoo_symbol(self, symbol: str) -> str:
        """Convert Indian ETF symbol to Yahoo Finance format."""
        return f"{symbol}.NS"

    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate downloaded data for completeness and quality."""
        if data.empty:
            logger.warning("Data is empty")
            return False
        
        # Check for required columns (when auto_adjust=True, Close is already adjusted)
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            logger.warning(f"Missing required columns: {missing_columns}")
            logger.warning(f"Available columns: {list(data.columns)}")
            return False
        
        # Check for valid OHLC data (no negative prices, but allow zero for some ETFs)
        price_columns = ['Open', 'High', 'Low', 'Close']
        for col in price_columns:
            if (data[col] < 0).any():
                logger.warning(f"Invalid price data found in {col} (negative prices)")
                return False
            # Log warning for zero prices but don't fail validation
            if (data[col] == 0).any():
                zero_count = (data[col] == 0).sum()
                logger.warning(f"Found {zero_count} zero prices in {col} for ETF data")
        
        # Check for reasonable volume data
        if (data['Volume'] < 0).any():
            logger.warning("Invalid volume data found (negative volume)")
            return False
        
        return True

    def download_etf_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Download historical data for a single ETF with retry logic."""
        yahoo_symbol = self.get_yahoo_symbol(symbol)
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Downloading {symbol} (attempt {attempt + 1}/{self.max_retries})")
                
                # Create ticker object
                ticker = yf.Ticker(yahoo_symbol)
                
                # Download data (yfinance end is exclusive, so add 1 day to include end_date)
                end_date_obj = datetime.strptime(self.end_date, "%Y-%m-%d")
                end_date_inclusive = (end_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
                
                data = ticker.history(
                    start=self.start_date,
                    end=end_date_inclusive,
                    auto_adjust=True,
                    prepost=False
                )
                
                if data.empty:
                    logger.warning(f"No data found for {symbol}")
                    return None
                
                # Debug: Log data info
                logger.info(f"Downloaded data for {symbol}: {len(data)} rows, columns: {list(data.columns)}")
                
                # Validate data
                if not self.validate_data(data):
                    logger.warning(f"Data validation failed for {symbol}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                        continue
                    return None
                
                # Clean and prepare data
                data = data.reset_index()
                data['Date'] = pd.to_datetime(data['Date']).dt.date   
                data = data.rename(columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                })
                
                # When auto_adjust=True, Close is already adjusted, so we duplicate it as adjusted_close
                data['adjusted_close'] = data['close']
                
                # Add symbol column
                data['symbol'] = symbol
                
                # Reorder columns
                data = data[['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']]
                
                logger.info(f"Successfully downloaded {len(data)} records for {symbol}")
                return data
                
            except Exception as e:
                logger.error(f"Error downloading {symbol} (attempt {attempt + 1}): {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to download {symbol} after {self.max_retries} attempts")
                    return None

    def save_etf_data(self, data: pd.DataFrame, symbol: str):
        """Save ETF data to PostgreSQL database using batch processing."""
        # Validate that symbol is in ETF list
        if symbol not in self.etf_symbols:
            logger.warning(f"Symbol {symbol} is not in ETF list, skipping save to etf_data table")
            return
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Insert or update security info
            cursor.execute('''
                INSERT INTO etf_info (symbol, name, type)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name,
                    type = EXCLUDED.type
            ''', (symbol, symbol, "ETF"))
            
            # Insert security data in batches using execute_batch for better performance
            records = []
            for _, row in data.iterrows():
                records.append((
                    row['symbol'],
                    row['date'],
                    float(row['open']) if pd.notna(row['open']) else None,
                    float(row['high']) if pd.notna(row['high']) else None,
                    float(row['low']) if pd.notna(row['low']) else None,
                    float(row['close']) if pd.notna(row['close']) else None,
                    float(row['adjusted_close']) if pd.notna(row['adjusted_close']) else None,
                    int(row['volume']) if pd.notna(row['volume']) else None
                ))
            
            if not records:
                logger.warning(f"No records to save for {symbol}")
                return
            
            logger.info(f"{symbol}: Attempting to save {len(records)} records to database")
            logger.debug(f"{symbol}: Sample records (first 3): {records[:3] if len(records) >= 3 else records}")
            
            # Use execute_batch for efficient batch insertion
            execute_batch(cursor, '''
                INSERT INTO etf_data 
                (symbol, date, open, high, low, close, adjusted_close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO NOTHING
            ''', records, page_size=1000)
            
            conn.commit()
            
            # Verify how many records were actually inserted
            dates_to_check = [row[1] for row in records]
            if dates_to_check:
                # Use ANY for PostgreSQL IN clause with list
                cursor.execute('''
                    SELECT COUNT(*) 
                    FROM etf_data 
                    WHERE symbol = %s AND date = ANY(%s)
                ''', (symbol, dates_to_check))
                
                inserted_count = cursor.fetchone()[0]
                logger.info(f"{symbol}: Attempted to save {len(records)} records, verified {inserted_count} records in database")
                
                if inserted_count < len(records):
                    missing_count = len(records) - inserted_count
                    logger.warning(f"{symbol}: Only {inserted_count} out of {len(records)} records were saved. {missing_count} records may have been duplicates or failed.")
                    
                    # Check which dates are missing
                    cursor.execute('''
                        SELECT date 
                        FROM etf_data 
                        WHERE symbol = %s AND date = ANY(%s)
                    ''', (symbol, dates_to_check))
                    saved_dates = {row[0] for row in cursor.fetchall()}
                    expected_dates = {row[1] for row in records}
                    missing_dates_in_db = expected_dates - saved_dates
                    if missing_dates_in_db:
                        logger.warning(f"{symbol}: Dates not found in database after save: {sorted(missing_dates_in_db)}")
                else:
                    logger.info(f"{symbol}: Successfully saved all {len(records)} records to database")
            
        except Exception as e:
            logger.error(f"Error saving data for {symbol}: {str(e)}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def get_downloaded_symbols(self) -> set:
        """Get list of already downloaded symbols (only ETFs from the symbols list)."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT DISTINCT symbol FROM etf_data')
            all_symbols = {row[0] for row in cursor.fetchall()}
            # Filter to only include symbols from etf_symbols list
            etf_symbols_set = set(self.etf_symbols)
            filtered_symbols = all_symbols.intersection(etf_symbols_set)
            logger.debug(f"Found {len(all_symbols)} total symbols in database, {len(filtered_symbols)} are ETFs from symbols list")
            return filtered_symbols
        finally:
            cursor.close()
            conn.close()

    def get_last_date_for_symbol(self, symbol: str) -> Optional[str]:
        """Get the last date that has data for a symbol in the database.
        This gets the absolute last date from etf_data table, independent of end_date.
        
        Returns:
            Last date as string (YYYY-MM-DD) or None if no data exists
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT MAX(date) 
                FROM etf_data 
                WHERE symbol = %s
            ''', (symbol,))
            
            result = cursor.fetchone()
            if result and result[0]:
                last_date = result[0].strftime("%Y-%m-%d")
                logger.debug(f"{symbol}: Last date found in database: {last_date}")
                return last_date
            logger.debug(f"{symbol}: No data found in database")
            return None
        finally:
            cursor.close()
            conn.close()

    def get_missing_dates_for_symbol(self, symbol: str, start_date: str, end_date: str) -> List[str]:
        """Get list of missing dates for a symbol within a date range.
        Uses actual trading days from yfinance, not business days frequency.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get all dates that exist for this symbol
            cursor.execute('''
                SELECT DISTINCT date 
                FROM etf_data 
                WHERE symbol = %s AND date >= %s AND date <= %s
                ORDER BY date
            ''', (symbol, start_date, end_date))
            
            existing_dates = {row[0] for row in cursor.fetchall()}
            
            # Get actual trading days from yfinance (not using 'B' frequency)
            yahoo_symbol = self.get_yahoo_symbol(symbol)
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            end_date_inclusive = (end_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            
            try:
                ticker = yf.Ticker(yahoo_symbol)
                data = ticker.history(
                    start=start_date,
                    end=end_date_inclusive,
                    auto_adjust=True,
                    prepost=False
                )
                
                if data.empty:
                    return []
                
                # Get actual trading days from the data
                data = data.reset_index()
                data['Date'] = pd.to_datetime(data['Date']).dt.date
                trading_dates = {date for date in data['Date']}
                
            except Exception as e:
                logger.warning(f"Error fetching trading days for {symbol}: {e}")
                return []
            
            # Find missing dates (trading days that don't exist in database)
            missing_dates = sorted(trading_dates - existing_dates)
            
            return [str(date) for date in missing_dates]
        finally:
            cursor.close()
            conn.close()

    def update_missing_dates(self, start_date: str = "2025-11-01", end_date: str = "2025-11-07"):
        """Download missing dates for all ETF symbols that already have data."""
        logger.info(f"Checking for missing dates between {start_date} and {end_date}")
        
        downloaded_symbols = self.get_downloaded_symbols()
        # Additional filter: only process symbols from etf_symbols list
        etf_symbols_set = set(self.etf_symbols)
        downloaded_symbols = downloaded_symbols.intersection(etf_symbols_set)
        
        if not downloaded_symbols:
            logger.warning("No ETF symbols found in database. Run download_all_etfs() first.")
            return
        
        logger.info(f"Checking {len(downloaded_symbols)} ETF symbols for missing dates...")
        
        successful_updates = 0
        failed_updates = 0
        
        for symbol in sorted(downloaded_symbols):
            missing_dates = self.get_missing_dates_for_symbol(symbol, start_date, end_date)
            
            if not missing_dates:
                logger.debug(f"{symbol}: No missing dates")
                continue
            
            logger.info(f"{symbol}: Missing {len(missing_dates)} dates: {', '.join(missing_dates)}")
            
            # Download data for the date range (yfinance end is exclusive, so add 1 day)
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            end_date_inclusive = (end_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            
            yahoo_symbol = self.get_yahoo_symbol(symbol)
            
            for attempt in range(self.max_retries):
                try:
                    ticker = yf.Ticker(yahoo_symbol)
                    data = ticker.history(
                        start=start_date,
                        end=end_date_inclusive,  # Exclusive end, so add 1 day
                        auto_adjust=True,
                        prepost=False
                    )
                    
                    if data.empty:
                        logger.warning(f"No data found for {symbol} in date range")
                        break
                    
                    if self.validate_data(data):
                        # Process and save data
                        data = data.reset_index()
                        data['Date'] = pd.to_datetime(data['Date']).dt.date
                        
                        # Filter only missing dates
                        data = data[data['Date'].isin([datetime.strptime(d, "%Y-%m-%d").date() for d in missing_dates])]
                        
                        if not data.empty:
                            data = data.rename(columns={
                                'Date': 'date',
                                'Open': 'open',
                                'High': 'high',
                                'Low': 'low',
                                'Close': 'close',
                                'Volume': 'volume'
                            })
                            data['adjusted_close'] = data['close']
                            data['symbol'] = symbol
                            data = data[['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']]
                            
                            self.save_etf_data(data, symbol)
                            logger.info(f"Successfully updated {len(data)} missing dates for {symbol}")
                            successful_updates += 1
                        else:
                            logger.warning(f"No new data found for {symbol} in missing date range")
                    break
                    
                except Exception as e:
                    logger.error(f"Error updating {symbol} (attempt {attempt + 1}): {str(e)}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                    else:
                        failed_updates += 1
            
            time.sleep(self.delay_between_requests)
        
        logger.info(f"Update completed! Successful: {successful_updates}, Failed: {failed_updates}")

    def download_all_etfs(self, end_date: str = None):
        """Download data for all ETFs with progress tracking.
        Only downloads missing trading days for each symbol.
        
        Args:
            end_date: User-defined end date (YYYY-MM-DD). If None, uses today's date in IST.
        """
        if end_date is None:
            # Use today's date in IST timezone
            end_date = datetime.now(IST).strftime("%Y-%m-%d")
        
        total_symbols = len(self.all_symbols)
        
        logger.info(f"Total ETF symbols: {total_symbols}")
        logger.info(f"End date: {end_date}")
        logger.info("Checking for missing trading days for each symbol...")
        
        successful_downloads = 0
        failed_downloads = 0
        skipped_symbols = 0
        
        for i, symbol in enumerate(self.all_symbols, 1):
            # Double-check symbol is in ETF list
            if symbol not in self.etf_symbols:
                logger.warning(f"Skipping {symbol} - not in ETF symbols list")
                skipped_symbols += 1
                continue
            logger.info(f"Progress: {i}/{total_symbols} - Processing {symbol}")
            
            # Step 1: Get last date for this symbol from database (independent of end_date)
            last_date = self.get_last_date_for_symbol(symbol)
            
            # Step 2: Determine start_date based on last date in database
            if last_date:
                # Start from the day after the last date found in database
                last_date_obj = datetime.strptime(last_date, "%Y-%m-%d")
                start_date = (last_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
                logger.info(f"{symbol}: Last date in database: {last_date}, will start from: {start_date} (user-defined end_date: {end_date})")
            else:
                # No data exists in database, start from initial start_date
                start_date = self.start_date
                logger.info(f"{symbol}: No existing data in database, starting from: {start_date} (user-defined end_date: {end_date})")
            
            # Step 3: Check if start_date is after end_date (user-defined)
            if start_date > end_date:
                logger.info(f"{symbol}: Start date ({start_date}) is after user-defined end date ({end_date}), skipping")
                skipped_symbols += 1
                continue
            
            # Step 4: Check for missing trading days between start_date (from DB) and end_date (user-defined)
            logger.info(f"{symbol}: Checking for missing trading days between {start_date} and {end_date}")
            missing_dates = self.get_missing_dates_for_symbol(symbol, start_date, end_date)
            
            if not missing_dates:
                logger.info(f"{symbol}: All trading days already exist in database, skipping")
                skipped_symbols += 1
                continue
            
            logger.info(f"{symbol}: Found {len(missing_dates)} missing trading days, downloading...")
            
            # Download data for the date range
            yahoo_symbol = self.get_yahoo_symbol(symbol)
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            end_date_inclusive = (end_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            
            data = None
            for attempt in range(self.max_retries):
                try:
                    ticker = yf.Ticker(yahoo_symbol)
                    data = ticker.history(
                        start=start_date,
                        end=end_date_inclusive,
                        auto_adjust=True,
                        prepost=False
                    )
                    break
                except Exception as e:
                    logger.error(f"Error downloading {symbol} (attempt {attempt + 1}): {str(e)}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
            
            if data is not None and not data.empty:
                # Validate data
                if not self.validate_data(data):
                    logger.warning(f"Data validation failed for {symbol}")
                    failed_downloads += 1
                    continue
                
                # Process data
                data = data.reset_index()
                data['Date'] = pd.to_datetime(data['Date']).dt.date
                data = data.rename(columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                })
                data['adjusted_close'] = data['close']
                data['symbol'] = symbol
                data = data[['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']]
                
                # Filter to only missing dates before saving
                missing_dates_set = {datetime.strptime(d, "%Y-%m-%d").date() for d in missing_dates}
                logger.info(f"{symbol}: Missing dates expected: {missing_dates}")
                logger.info(f"{symbol}: Downloaded data dates: {[str(d) for d in sorted(data['date'].unique())]}")
                
                data_filtered = data[data['date'].isin(missing_dates_set)]
                
                logger.info(f"{symbol}: Downloaded {len(data)} rows, filtered to {len(data_filtered)} rows matching missing dates")
                
                if data_filtered.empty:
                    logger.warning(f"{symbol}: No data found for missing dates after filtering")
                    logger.warning(f"{symbol}: Missing dates set: {sorted(missing_dates_set)}")
                    logger.warning(f"{symbol}: Dates in downloaded data: {[str(d) for d in sorted(data['date'].unique())]}")
                    failed_downloads += 1
                    continue
                
                logger.info(f"{symbol}: Filtered to {len(data_filtered)} missing dates to save")
                logger.info(f"{symbol}: Dates to be saved: {[str(d) for d in sorted(data_filtered['date'].unique())]}")
                
                # Save only missing dates to database (ON CONFLICT will prevent duplicates)
                self.save_etf_data(data_filtered, symbol)
                successful_downloads += 1
            else:
                failed_downloads += 1
                logger.warning(f"Failed to download data for {symbol}")
            
            # Rate limiting
            if i < total_symbols:  # Don't delay after last request
                time.sleep(self.delay_between_requests)
        
        logger.info(f"Download completed!")
        logger.info(f"Successful downloads: {successful_downloads}")
        logger.info(f"Failed downloads: {failed_downloads}")
        logger.info(f"Skipped (all dates already exist): {skipped_symbols}")

    def get_database_stats(self):
        """Get statistics about downloaded data."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get total records
            cursor.execute('SELECT COUNT(*) FROM etf_data')
            total_records = cursor.fetchone()[0]
            
            # Get unique symbols
            cursor.execute('SELECT COUNT(DISTINCT symbol) FROM etf_data')
            unique_symbols = cursor.fetchone()[0]
            
            # Get date range
            cursor.execute('SELECT MIN(date), MAX(date) FROM etf_data')
            date_range = cursor.fetchone()
            
            # Get records per symbol
            cursor.execute('''
                SELECT symbol, COUNT(*) as record_count 
                FROM etf_data 
                GROUP BY symbol 
                ORDER BY record_count DESC 
                LIMIT 10
            ''')
            top_symbols = cursor.fetchall()
            
            # Get breakdown by type
            cursor.execute('''
                SELECT s.type, COUNT(DISTINCT s.symbol) as symbol_count
                FROM etf_info s
                WHERE s.symbol IN (SELECT DISTINCT symbol FROM etf_data)
                GROUP BY s.type
            ''')
            type_breakdown = cursor.fetchall()
            
            logger.info("=== Database Statistics ===")
            logger.info(f"Total records: {total_records:,}")
            logger.info(f"Unique symbols: {unique_symbols}")
            logger.info(f"Date range: {date_range[0]} to {date_range[1]}")
            logger.info("Breakdown by type:")
            for sec_type, count in type_breakdown:
                logger.info(f"  {sec_type}: {count} symbols")
            logger.info("Top 10 symbols by record count:")
            for symbol, count in top_symbols:
                logger.info(f"  {symbol}: {count:,} records")
                
        finally:
            cursor.close()
            conn.close()

    def cleanup_stock_symbols(self):
        """Remove any stock symbols from etf_data and etf_info tables.
        Only keeps symbols that are in the etf_symbols list.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get all symbols currently in the database
            cursor.execute('SELECT DISTINCT symbol FROM etf_data')
            all_symbols_in_db = {row[0] for row in cursor.fetchall()}
            
            # Get ETF symbols set
            etf_symbols_set = set(self.etf_symbols)
            
            # Find symbols that are NOT in the ETF list (these are stock symbols to remove)
            stock_symbols_to_remove = all_symbols_in_db - etf_symbols_set
            
            if not stock_symbols_to_remove:
                logger.info("No stock symbols found in ETF table. Database is clean.")
                return
            
            logger.warning(f"Found {len(stock_symbols_to_remove)} non-ETF symbols in etf_data table: {sorted(stock_symbols_to_remove)}")
            logger.info("Removing stock symbols from etf_data and etf_info tables...")
            
            # Delete from etf_data table
            for symbol in stock_symbols_to_remove:
                cursor.execute('DELETE FROM etf_data WHERE symbol = %s', (symbol,))
                deleted_count = cursor.rowcount
                logger.info(f"Deleted {deleted_count} records for symbol {symbol} from etf_data")
            
            # Delete from etf_info table
            for symbol in stock_symbols_to_remove:
                cursor.execute('DELETE FROM etf_info WHERE symbol = %s', (symbol,))
                if cursor.rowcount > 0:
                    logger.info(f"Deleted symbol {symbol} from etf_info")
            
            conn.commit()
            logger.info(f"Cleanup completed! Removed {len(stock_symbols_to_remove)} stock symbols from ETF tables.")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def get_etf_info(self, symbol: str):
        """Get additional information about an ETF."""
        yahoo_symbol = self.get_yahoo_symbol(symbol)
        ticker = yf.Ticker(yahoo_symbol)
        
        try:
            info = ticker.info
            logger.info(f"=== {symbol} Information ===")
            logger.info(f"Name: {info.get('longName', 'N/A')}")
            logger.info(f"Sector: {info.get('sector', 'N/A')}")
            logger.info(f"Industry: {info.get('industry', 'N/A')}")
            logger.info(f"Market Cap: {info.get('marketCap', 'N/A')}")
            logger.info(f"Description: {info.get('longBusinessSummary', 'N/A')[:200]}...")
        except Exception as e:
            logger.warning(f"Could not fetch info for {symbol}: {str(e)}")

def main():
    """Main function to run the ETF data downloader."""
    import sys
    
    downloader = ETFDataDownloader()
    
    try:
        # Check for cleanup flag
        if len(sys.argv) > 1 and sys.argv[1] == "--cleanup":
            logger.info("Cleaning up stock symbols from ETF tables...")
            downloader.cleanup_stock_symbols()
            logger.info("Cleanup completed!")
            return
        
        # Get end_date from command line argument if provided
        end_date = None
        if len(sys.argv) > 1:
            if sys.argv[1] == "--update":
                # Update mode - use default end_date
                logger.info("Updating missing dates...")
                downloader.download_all_etfs()
            elif sys.argv[1].startswith("--end-date="):
                # Custom end_date provided
                end_date = sys.argv[1].split("=")[1]
                logger.info(f"Using custom end date: {end_date}")
                downloader.download_all_etfs(end_date=end_date)
            else:
                # Default: download all with default end_date
                downloader.download_all_etfs()
        else:
            # Default: download all with default end_date
            logger.info("Starting ETF Data Downloader")
            logger.info(f"Date range: {downloader.start_date} to {downloader.end_date}")
            downloader.download_all_etfs()
        
        # Show statistics
        downloader.get_database_stats()
        
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        logger.info("ETF data downloader finished")

if __name__ == "__main__":
    main()

