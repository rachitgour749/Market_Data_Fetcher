#!/usr/bin/env python3
"""
US ETF Data Downloader
Downloads OHLC data for 102 US ETFs from Yahoo Finance and stores in 'international_etf_data' table.
"""

import psycopg2
from psycopg2.extras import execute_batch
import yfinance as yf
import pandas as pd
import time
import logging
import sys
from datetime import datetime
import pytz

# Database connection string
DATABASE_URL = "postgresql://neondb_owner:npg_WgVhOYtnP12l@ep-solitary-silence-a1yoj91r.ap-southeast-1.aws.neon.tech/MarketData?sslmode=require&channel_binding=require"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('us_etf_download.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class USEtfDownloader:
    def __init__(self):
        # Start from a reasonable historical date, e.g., 2000-01-01
        self.start_date = "2000-01-01"  
        self.end_date = datetime.now().strftime("%Y-%m-%d")
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        # New US ETFs List from user's image
        self.etf_symbols = [
            # Broad Market
            "SPY", "IVV", "VOO", "VTI", "QQQ", "DIA", "IWM", "VT", "SCHB", "RSP",
            # Sector - Financials
            "XLF", "VFH", "IYF", "KBE", "KRE", "IAF",
            # Sector - Technology  
            "XLK", "VGT", "IGV", "FDN",
            # Sector - Healthcare
            "XLV", "VHT", "IHE", "PJP", "XBI", "IBB", "FXH",
            # Sector - Energy
            "XLE", "VDE", "IYE", "OIH", "XOP", "IEO", "PXI",
            # Sector - Materials
            "XLB", "IYM", "GDX", "GDXJ", "SLV", "GLD", "BAR", "PLTM", "PALL", "PDBC",
            # Sector - Consumer Discretionary
            "XLY", "VCR", "IYC", "ITB", "XHB",
            # Sector - Consumer Staples
            "XLP", "VDC",
            # Sector - Utilities
            "XLU", "IDU",
            # Sector - Real Estate
            "VNQ", "IYR", "XLRE",
            # Sector - Communication
            "XLC", "VOX",
            # Sector - Industrials
            "XLI",
            # Transportation
            "IYT", "XTN", "JETS",
            # Automotive
            "CARZ", "MOTOR", "IDRV",
            # International
            "EEM", "VEA",
            # Bonds
            "AGG", "BND", "BSV", "VGSH", "SPTS", "VTIP", "IEI", "SHY", "MINT", "NEAR", "BIL",
            # Commodities
            "USO", "DJP", "DBA", "DBB", "JO", "JJU", "DBC",
            # Thematic/Other
            "ITA", "CHAT", "TAN", "IBAT", "NLR", "SUPP", "UCO", "UNG", "SMDV", "NOBL", 
            "OILK", "GLDM", "SCHD", "DIVO", "MOAT",
            # Short/Inverse ETFs
            "SH", "PSQ", "SDS", "SPXS", "SPXU", "QID", "SQQQ", "DOG", "DXD", "SDOW",
            "SEF", "SKF", "FAZ", "SSG", "SOXS", "TECS", "SRS", "DUG", "TBT", "TMV", "SARK"
        ]
        
        # Remove duplicates just in case
        self.etf_symbols = sorted(list(set(self.etf_symbols)))
        logger.info(f"Initialized downloader with {len(self.etf_symbols)} unique ETF symbols")

    def get_connection(self):
        """Create and return a PostgreSQL database connection."""
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL database: {str(e)}")
            raise

    def get_last_date_for_symbol(self, symbol: str):
        """Get the last date that has data for a symbol in the database."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT MAX(date) FROM international_etf_data WHERE symbol = %s", (symbol,))
            result = cursor.fetchone()
            if result and result[0]:
                return result[0]
            return None
        finally:
            cursor.close()
            conn.close()

    def truncate_table(self):
        """Truncate the international_etf_data table to remove all old data."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            logger.info("Truncating international_etf_data table...")
            cursor.execute("TRUNCATE TABLE international_etf_data")
            conn.commit()
            logger.info("Successfully truncated international_etf_data table")
        except Exception as e:
            logger.error(f"Error truncating table: {str(e)}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def download_and_save_data(self):
        """Download all historical data for all ETFs from scratch (after truncate)."""
        logger.info("Starting full data download...")
        total_symbols = len(self.etf_symbols)
        successful = 0
        failed = 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            for i, symbol in enumerate(self.etf_symbols, 1):
                try:
                    logger.info(f"Downloading {i}/{total_symbols}: {symbol}")
                    
                    # Download data
                    data = None
                    for attempt in range(self.max_retries):
                        try:
                            data = yf.download(
                                symbol,
                                start=self.start_date,
                                end=None,  # Get all data up to latest
                                auto_adjust=False,
                                progress=False,
                                multi_level_index=False
                            )
                            break
                        except Exception as e:
                            logger.warning(f"Attempt {attempt + 1} failed for {symbol}: {e}")
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)
                    
                    if data is None or data.empty:
                        logger.warning(f"No data available for {symbol}")
                        failed += 1
                        continue
                    
                    # Process data
                    if isinstance(data.columns, pd.MultiIndex):
                        data.columns = data.columns.get_level_values(0)
                    
                    records = []
                    data = data.reset_index()
                    
                    for _, row in data.iterrows():
                        try:
                            date_val = row['Date'].strftime("%Y-%m-%d") if hasattr(row['Date'], 'strftime') else str(row['Date'])
                            
                            def get_val(col_name):
                                val = row.get(col_name)
                                if pd.notna(val):
                                    return round(float(val), 2)
                                return None
                            
                            vol = row.get('Volume')
                            vol = int(vol) if pd.notna(vol) else 0
                            
                            records.append((
                                symbol, date_val,
                                get_val('Open'), get_val('High'), get_val('Low'),
                                get_val('Close'), get_val('Adj Close'), vol,
                                'YAHOO', 'US', 'US', 'USD', 'USA', 'ETF'
                            ))
                        except Exception as e:
                            logger.warning(f"Error processing row for {symbol}: {e}")
                            continue
                    
                    if records:
                        execute_batch(cursor, '''
                            INSERT INTO international_etf_data 
                            (symbol, date, open, high, low, close, adjusted_close, volume, 
                             data_source, market, exchange, currency, country, etf_type)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''', records)
                        conn.commit()
                        logger.info(f"Inserted {len(records)} records for {symbol}")
                        successful += 1
                    else:
                        logger.warning(f"No valid records for {symbol}")
                        failed += 1
                    
                    time.sleep(0.5)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error downloading {symbol}: {e}")
                    failed += 1
                    conn.rollback()
        
        finally:
            cursor.close()
            conn.close()
        
        logger.info(f"Download completed. Successful: {successful}, Failed: {failed}")

    def update_daily_data(self, end_date=None):
        """Update data for all ETFs (incremental)."""
        logger.info(f"Starting daily incremental update (up to {end_date if end_date else 'today'})...")
        total_symbols = len(self.etf_symbols)
        successful = 0
        failed = 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            for i, symbol in enumerate(self.etf_symbols, 1):
                try:
                    logger.info(f"Checking {i}/{total_symbols}: {symbol}")
                    
                    # 1. Get last date
                    last_date = self.get_last_date_for_symbol(symbol)
                    
                    start_fetch_date = self.start_date
                    if last_date:
                        # Start from next day
                        last_date_obj = last_date if isinstance(last_date, datetime) else datetime.strptime(str(last_date), "%Y-%m-%d").date()
                        start_fetch_date = (last_date_obj + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                        logger.info(f"Last date in DB: {last_date}, fetching from: {start_fetch_date}")
                        
                        # Check if already up to date (compare dates properly)
                        if end_date:
                            target_date = datetime.strptime(end_date, "%Y-%m-%d").date()
                        else:
                            target_date = datetime.now().date()
                            
                        start_date_obj = datetime.strptime(start_fetch_date, "%Y-%m-%d").date()
                        if start_date_obj > target_date:
                            logger.info(f"Up to date. Last: {last_date}, Start would be: {start_fetch_date}, Target: {target_date}")
                            successful += 1
                            continue
                    else:
                        logger.info(f"No existing data for {symbol}, fetching from {start_fetch_date}")
                    
                    logger.info(f"Fetching {symbol} from {start_fetch_date}")
                    
                    # 2. Download latest data
                    data = None
                    for attempt in range(self.max_retries):
                        try:
                            # Use end=None to get latest available data
                            # yfinance end parameter is exclusive, so using today's date would miss today's data
                            # yf.download end parameter is exclusive, so add 1 day if end_date is provided
                            fetch_end = None
                            if end_date:
                                fetch_end = (datetime.strptime(end_date, "%Y-%m-%d") + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                                
                            data = yf.download(
                                symbol, 
                                start=start_fetch_date, 
                                end=fetch_end, 
                                auto_adjust=False,
                                progress=False,
                                multi_level_index=False
                            )
                            break
                        except Exception as e:
                            time.sleep(self.retry_delay)
                            
                    if data is None or data.empty:
                        logger.info(f"No new data for {symbol}")
                        successful += 1 # Not a failure, just no data (holiday/weekend)
                        continue
                        
                    # 3. Process and Insert (Reusing logic from download_and_save_data would be better, but copying for safety/speed now)
                    if isinstance(data.columns, pd.MultiIndex):
                         data.columns = data.columns.get_level_values(0)

                    records = []
                    data = data.reset_index()
                    
                    for _, row in data.iterrows():
                        try:
                            date_val = row['Date'].strftime("%Y-%m-%d") if hasattr(row['Date'], 'strftime') else str(row['Date'])
                            
                            # Deduplication check: strict constraint on (symbol, date) might trigger error if we don't check.
                            # We trust get_last_date, but yfinance might return overlapping day if timezones weird.
                            # We'll rely on ON CONFLICT DO NOTHING or just try/except.
                            # Or better: filter data > last_date locally.
                            if last_date and str(date_val) <= str(last_date):
                                continue

                            def get_val(col_name):
                                val = row.get(col_name)
                                if pd.notna(val):
                                    return round(float(val), 2)
                                return None
                                
                            vol = row.get('Volume')
                            vol = int(vol) if pd.notna(vol) else 0

                            records.append((
                                symbol, date_val,
                                get_val('Open'), get_val('High'), get_val('Low'),
                                get_val('Close'), get_val('Adj Close'), vol,
                                'YAHOO', 'US', 'US', 'USD', 'USA', 'ETF'
                            ))
                        except: continue

                    if records:
                        execute_batch(cursor, '''
                            INSERT INTO international_etf_data 
                            (symbol, date, open, high, low, close, adjusted_close, volume, 
                             data_source, market, exchange, currency, country, etf_type)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (symbol, date) DO UPDATE SET
                                close = EXCLUDED.close,
                                adjusted_close = EXCLUDED.adjusted_close,
                                volume = EXCLUDED.volume
                        ''', records)
                        conn.commit()
                        logger.info(f"Appended {len(records)} records for {symbol}")
                    
                    successful += 1
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error updating {symbol}: {e}")
                    failed += 1
                    conn.rollback()

        finally:
            cursor.close()
            conn.close()
            
        logger.info(f"Update completed. Successful: {successful}, Failed: {failed}")

def main():
    import sys
    downloader = USEtfDownloader()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--full-refresh':
            logger.info("Performing FULL REFRESH (Truncate & Download)...")
            downloader.truncate_table()
            downloader.download_and_save_data()
        elif sys.argv[1].startswith("--end-date="):
            end_date = sys.argv[1].split("=")[1]
            logger.info(f"Performing DAILY UPDATE up to {end_date}...")
            downloader.update_daily_data(end_date=end_date)
        else:
            logger.info("Performing DAILY UPDATE (Incremental)...")
            downloader.update_daily_data()
    else:
        logger.info("Performing DAILY UPDATE (Incremental)...")
        downloader.update_daily_data()

if __name__ == "__main__":
    main()
