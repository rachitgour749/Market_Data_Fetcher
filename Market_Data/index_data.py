#!/usr/bin/env python3
"""
NIFTY 50 Index Data Downloader
Downloads historical OHLC data for NIFTY 50 index from Yahoo Finance and stores in PostgreSQL database.
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
        logging.FileHandler('index_data.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class IndexDataDownloader:
    def __init__(self, db_url: str = DATABASE_URL):
        self.db_url = db_url
        self.start_date = "2001-01-01"  # Keep for initial historical loads
        # Default end_date to today in IST timezone
        self.end_date = datetime.now(IST).strftime("%Y-%m-%d")
        self.delay_between_requests = 1.5  # seconds
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        # Initialize database
        self.init_database()
        
        # NIFTY 50 symbol
        self.nifty50_symbol = "^NSEI"  # Yahoo Finance symbol for NIFTY 50

    def init_database(self):
        """Initialize PostgreSQL database with proper schema."""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            # Create index_info table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS index_info (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT UNIQUE NOT NULL,
                    name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            # Create index_data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS index_data (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    adj_close REAL,
                    volume INTEGER,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(symbol, date),
                    FOREIGN KEY (symbol) REFERENCES index_info (symbol)
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_date ON index_data (symbol, date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON index_data (symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON index_data (date)')
            
            conn.commit()
            logger.info("Database initialized successfully")
            
        except psycopg2.Error as e:
            logger.error(f"Error initializing database: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate downloaded data for completeness and quality."""
        if data.empty:
            logger.warning("Data is empty")
            return False
        
        # Check for required columns
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            logger.warning(f"Missing required columns: {missing_columns}")
            logger.warning(f"Available columns: {list(data.columns)}")
            return False
        
        # Check for valid OHLC data (no negative prices)
        price_columns = ['Open', 'High', 'Low', 'Close']
        for col in price_columns:
            if (data[col] <= 0).any():
                logger.warning(f"Invalid price data found in {col} (negative or zero prices)")
                return False
        
        # Check for reasonable volume data
        if (data['Volume'] < 0).any():
            logger.warning("Invalid volume data found (negative volume)")
            return False
        
        return True

    def download_nifty50_data(self) -> Optional[pd.DataFrame]:
        """Download historical data for NIFTY 50 index with retry logic."""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Downloading NIFTY 50 (attempt {attempt + 1}/{self.max_retries})")
                
                # Create ticker object
                ticker = yf.Ticker(self.nifty50_symbol)
                
                # Download data with both adjusted and unadjusted close
                # yfinance end is exclusive, so add 1 day to include end_date
                end_date_obj = datetime.strptime(self.end_date, "%Y-%m-%d")
                end_date_inclusive = (end_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
                
                data = ticker.history(
                    start=self.start_date,
                    end=end_date_inclusive,
                    auto_adjust=False,  # Get both adjusted and unadjusted data
                    prepost=False
                )
                
                if data.empty:
                    logger.warning("No data found for NIFTY 50")
                    return None
                
                # Debug: Log data info
                logger.info(f"Downloaded data for NIFTY 50: {len(data)} rows, columns: {list(data.columns)}")
                
                # Validate data
                if not self.validate_data(data):
                    logger.warning("Data validation failed for NIFTY 50")
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
                
                # Add adjusted close (use Close as adjusted close for index data)
                if 'Adj Close' in data.columns:
                    data['adj_close'] = data['Adj Close']
                else:
                    # If no Adj Close column, use Close as adjusted close
                    data['adj_close'] = data['close']
                
                # Add symbol column
                data['symbol'] = 'NIFTY50'
                
                # Reorder columns
                data = data[['symbol', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
                
                logger.info(f"Successfully downloaded {len(data)} records for NIFTY 50")
                return data
                
            except Exception as e:
                logger.error(f"Error downloading NIFTY 50 (attempt {attempt + 1}): {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to download NIFTY 50 after {self.max_retries} attempts")
                    return None

    def save_nifty50_data(self, data: pd.DataFrame):
        """Save NIFTY 50 data to database using batch processing."""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            # Insert or update index info
            cursor.execute('''
                INSERT INTO index_info (symbol, name)
                VALUES (%s, %s)
                ON CONFLICT (symbol) DO UPDATE SET name = EXCLUDED.name
            ''', ('NIFTY50', 'NIFTY 50 Index'))
            
            # Insert index data in batches to avoid "too many SQL variables" error
            batch_size = 1000  # Process 1000 records at a time
            total_records = len(data)
            
            for i in range(0, total_records, batch_size):
                batch_data = data.iloc[i:i + batch_size]
                
                # Prepare data for batch insert
                records = []
                for _, row in batch_data.iterrows():
                    records.append((
                        row['symbol'],
                        row['date'],
                        row['open'],
                        row['high'],
                        row['low'],
                        row['close'],
                        row['adj_close'],
                        row['volume']
                    ))
                
                # Insert batch using execute_batch for better performance
                execute_batch(cursor, '''
                    INSERT INTO index_data 
                    (symbol, date, open, high, low, close, adj_close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, date) DO NOTHING
                ''', records, page_size=batch_size)
            
            conn.commit()
            logger.info(f"Saved {len(data)} records for NIFTY 50 to database")
            
        except psycopg2.Error as e:
            logger.error(f"Error saving data for NIFTY 50: {str(e)}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def get_downloaded_data(self) -> bool:
        """Check if NIFTY 50 data is already downloaded."""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT COUNT(*) FROM index_data WHERE symbol = %s', ('NIFTY50',))
            count = cursor.fetchone()[0]
            return count > 0
        except psycopg2.Error as e:
            logger.error(f"Error checking downloaded data: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def get_last_date_for_symbol(self) -> Optional[str]:
        """Get the last (most recent) date for NIFTY50 in the database.
        
        Returns:
            Last date as string (YYYY-MM-DD) or None if no data exists
        """
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT MAX(date) 
                FROM index_data 
                WHERE symbol = %s
            ''', ('NIFTY50',))
            
            result = cursor.fetchone()
            if result and result[0]:
                return result[0].strftime("%Y-%m-%d")
            return None
        except psycopg2.Error as e:
            logger.error(f"Error getting last date for NIFTY50: {e}")
            return None
        finally:
            cursor.close()
            conn.close()

    def get_missing_dates(self, start_date: str, end_date: str) -> List[str]:
        """Get list of missing dates for NIFTY50 within a date range."""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            # Get all dates that exist for NIFTY50
            cursor.execute('''
                SELECT DISTINCT date 
                FROM index_data 
                WHERE symbol = %s AND date >= %s AND date <= %s
                ORDER BY date
            ''', ('NIFTY50', start_date, end_date))
            
            existing_dates = {row[0] for row in cursor.fetchall()}
            
            # Generate all trading dates in range (exclude weekends)
            date_range = pd.date_range(start=start_date, end=end_date, freq='B')  # 'B' = business days only
            all_dates = {date.date() for date in date_range}
            
            # Find missing dates
            missing_dates = sorted(all_dates - existing_dates)
            
            return [str(date) for date in missing_dates]
        except psycopg2.Error as e:
            logger.error(f"Error getting missing dates: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def update_missing_dates(self, start_date: str = None, end_date: str = None):
        """Download missing dates for NIFTY50.
        
        Args:
            start_date: Start date for checking missing dates. If None, uses last date in DB + 1 day.
            end_date: End date for checking missing dates. If None, uses today's date in IST.
        """
        # Get last date from database if start_date not provided
        if start_date is None:
            last_date = self.get_last_date_for_symbol()
            if last_date:
                last_date_obj = datetime.strptime(last_date, "%Y-%m-%d")
                start_date = (last_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = self.start_date
        
        # Use today's date if end_date not provided
        if end_date is None:
            end_date = datetime.now(IST).strftime("%Y-%m-%d")
        
        logger.info(f"Checking for missing dates between {start_date} and {end_date}")
        
        missing_dates = self.get_missing_dates(start_date, end_date)
        
        if not missing_dates:
            logger.info("No missing dates found for NIFTY50")
            return
        
        logger.info(f"NIFTY50: Missing {len(missing_dates)} dates: {', '.join(missing_dates)}")
        
        # Download data for the date range (yfinance end is exclusive, so add 1 day)
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        end_date_inclusive = (end_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
        
        for attempt in range(self.max_retries):
            try:
                ticker = yf.Ticker(self.nifty50_symbol)
                data = ticker.history(
                    start=start_date,
                    end=end_date_inclusive,  # Exclusive end, so add 1 day
                    auto_adjust=False,
                    prepost=False
                )
                
                if data.empty:
                    logger.warning(f"No data found for NIFTY50 in date range")
                    return
                
                if self.validate_data(data):
                    # Process and save data
                    data = data.reset_index()
                    data['Date'] = pd.to_datetime(data['Date']).dt.date
                    
                    # Filter only missing dates
                    data = data[data['Date'].isin([datetime.strptime(d, "%Y-%m-%d").date() for d in missing_dates])]
                    
                    if not data.empty:
                        # Add adjusted close before renaming
                        if 'Adj Close' in data.columns:
                            adj_close = data['Adj Close'].copy()
                        else:
                            adj_close = data['Close'].copy()
                        
                        data = data.rename(columns={
                            'Date': 'date',
                            'Open': 'open',
                            'High': 'high',
                            'Low': 'low',
                            'Close': 'close',
                            'Volume': 'volume'
                        })
                        
                        data['adj_close'] = adj_close
                        
                        data['symbol'] = 'NIFTY50'
                        data = data[['symbol', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
                        
                        self.save_nifty50_data(data)
                        logger.info(f"Successfully updated {len(data)} missing dates for NIFTY50")
                        return
                    else:
                        logger.warning(f"No new data found for NIFTY50 in missing date range")
                break
                
            except Exception as e:
                logger.error(f"Error updating NIFTY50 (attempt {attempt + 1}): {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to update NIFTY50 after {self.max_retries} attempts")

    def download_nifty50(self):
        """Download NIFTY 50 data with progress tracking."""
        if self.get_downloaded_data():
            logger.info("NIFTY 50 data already downloaded!")
            return
        
        logger.info("Starting NIFTY 50 data download...")
        
        # Download data
        data = self.download_nifty50_data()
        
        if data is not None and not data.empty:
            # Save to database
            self.save_nifty50_data(data)
            logger.info("NIFTY 50 data download completed successfully!")
        else:
            logger.error("Failed to download NIFTY 50 data")

    def get_database_stats(self):
        """Get statistics about downloaded data."""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            # Get total records
            cursor.execute('SELECT COUNT(*) FROM index_data')
            total_records = cursor.fetchone()[0]
            
            # Get unique symbols
            cursor.execute('SELECT COUNT(DISTINCT symbol) FROM index_data')
            unique_symbols = cursor.fetchone()[0]
            
            # Get date range
            cursor.execute('SELECT MIN(date), MAX(date) FROM index_data')
            date_range = cursor.fetchone()
            
            # Get sample data
            cursor.execute('''
                SELECT date, open, high, low, close, adj_close, volume
                FROM index_data 
                ORDER BY date DESC 
                LIMIT 5
            ''')
            sample_data = cursor.fetchall()
            
            logger.info("=== Database Statistics ===")
            logger.info(f"Total records: {total_records:,}")
            logger.info(f"Unique symbols: {unique_symbols}")
            logger.info(f"Date range: {date_range[0]} to {date_range[1]}")
            logger.info("Recent 5 records:")
            for record in sample_data:
                logger.info(f"  {record[0]}: O={record[1]:.2f}, H={record[2]:.2f}, L={record[3]:.2f}, C={record[4]:.2f}, AC={record[5]:.2f}, V={record[6]:,}")
        except psycopg2.Error as e:
            logger.error(f"Error getting database stats: {e}")
        finally:
            cursor.close()
            conn.close()

    def get_nifty50_info(self):
        """Get additional information about NIFTY 50."""
        ticker = yf.Ticker(self.nifty50_symbol)
        
        try:
            info = ticker.info
            logger.info("=== NIFTY 50 Information ===")
            logger.info(f"Name: {info.get('longName', 'NIFTY 50 Index')}")
            logger.info(f"Currency: {info.get('currency', 'INR')}")
            logger.info(f"Exchange: {info.get('exchange', 'NSE')}")
            logger.info(f"Market State: {info.get('marketState', 'N/A')}")
            logger.info(f"Previous Close: {info.get('previousClose', 'N/A')}")
            logger.info(f"52 Week High: {info.get('fiftyTwoWeekHigh', 'N/A')}")
            logger.info(f"52 Week Low: {info.get('fiftyTwoWeekLow', 'N/A')}")
        except Exception as e:
            logger.warning(f"Could not fetch info for NIFTY 50: {str(e)}")

def main():
    """Main function to run the NIFTY 50 data downloader."""
    import sys
    
    downloader = IndexDataDownloader()
    
    try:
        if len(sys.argv) > 1:
            if sys.argv[1] == "--update":
                logger.info("Updating missing dates...")
                downloader.update_missing_dates()
            elif sys.argv[1].startswith("--end-date="):
                end_date = sys.argv[1].split("=")[1]
                logger.info(f"Updating missing dates up to {end_date}...")
                downloader.update_missing_dates(end_date=end_date)
            else:
                logger.info("Starting NIFTY 50 Data Downloader")
                logger.info(f"Date range: {downloader.start_date} to {downloader.end_date}")
                downloader.download_nifty50()
                logger.info("Checking for missing dates...")
                downloader.update_missing_dates()
        else:
            logger.info("Starting NIFTY 50 Data Downloader")
            logger.info(f"Date range: {downloader.start_date} to {downloader.end_date}")
            downloader.download_nifty50()
            logger.info("Checking for missing dates...")
            downloader.update_missing_dates()
        
        # Show statistics
        downloader.get_database_stats()
        
        # Show NIFTY 50 information
        logger.info("\n=== NIFTY 50 Information ===")
        downloader.get_nifty50_info()
        
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        logger.info("NIFTY 50 data downloader finished")

if __name__ == "__main__":
    main()

