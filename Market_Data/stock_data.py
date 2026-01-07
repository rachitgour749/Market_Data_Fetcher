#!/usr/bin/env python3
"""
Historical Stock Data Downloader
Downloads OHLC data for Indian stocks from Yahoo Finance and stores in PostgreSQL database.
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
        logging.FileHandler('stock_download.log'),
        logging.StreamHandler(sys.stdout) 
    ]
)
logger = logging.getLogger(__name__)

class StockDataDownloader:
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
        
        # Stock symbols list
        self.stock_symbols = [
            "360ONE", "3MINDIA", "AADHARHFC", "AARTIIND", "AAVAS", "ABB", "ABBOTINDIA",
            "ABCAPITAL", "ABFRL", "ABREL", "ABSLAMC", "ACC", "ACE", "ACMESOLAR",
            "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER",
            "AEGISLOG", "AFCONS", "AFFLE", "AIAENG", "AIIL", "AJANTPHARM", "AKUMS",
            "ALIVUS", "ALKEM", "ALKYLAMINE", "ALOKINDS", "AMBER", "AMBUJACEM",
            "ANANDRATHI", "ANANTRAJ", "ANGELONE", "APARINDS", "APLAPOLLO", "APLLTD",
            "APOLLOHOSP", "APOLLOTYRE", "APTUS", "ARE&M", "ASAHIINDIA", "ASHOKLEY",
            "ASIANPAINT", "ASTERDM", "ASTRAL", "ASTRAZEN", "ATGL", "ATUL", "AUBANK",
            "AUROPHARMA", "AWL", "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJAJHFL",
            "BAJAJHLDNG", "BAJFINANCE", "BALKRISIND", "BALRAMCHIN", "BANDHANBNK",
            "BANKBARODA", "BANKINDIA", "BASF", "BATAINDIA", "BAYERCROP", "BBTC",
            "BDL", "BEL", "BEML", "BERGEPAINT", "BHARATFORG", "BHARTIARTL",
            "BHARTIHEXA", "BHEL", "BIKAJI", "BIOCON", "BLS", "BLUEDART", "BLUESTARCO",
            "BOSCHLTD", "BPCL", "BRIGADE", "BRITANNIA", "BSE", "BSOFT", "CAMPUS",
            "CAMS", "CANBK", "CANFINHOME", "CAPLIPOINT", "CARBORUNIV", "CASTROLIND",
            "CCL", "CDSL", "CEATLTD", "CENTRALBK", "CENTURYPLY", "CERA", "CESC",
            "CGCL", "CGPOWER", "CHALET", "CHAMBLFERT", "CHENNPETRO", "CHOLAFIN",
            "CHOLAHLDNG", "CIPLA", "CLEAN", "COALINDIA", "COCHINSHIP", "COFORGE",
            "COLPAL", "CONCOR", "CONCORDBIO", "COROMANDEL", "CRAFTSMAN", "CREDITACC",
            "CRISIL", "CROMPTON", "CUB", "CUMMINSIND", "CYIENT", "DABUR", "DALBHARAT",
            "DATAPATTNS", "DBREALTY", "DCMSHRIRAM", "DEEPAKFERT", "DEEPAKNTR",
            "DELHIVERY", "DEVYANI", "DIVISLAB", "DIXON", "DLF", "DMART", "DOMS",
            "DRREDDY", "ECLERX", "EICHERMOT", "EIDPARRY", "EIHOTEL", "ELECON",
            "ELGIEQUIP", "EMAMILTD", "EMCURE", "ENDURANCE", "ENGINERSIN", "ERIS",
            "ESCORTS", "ETERNAL", "EXIDEIND", "FACT", "FEDERALBNK", "FINCABLES",
            "FINPIPE", "FIRSTCRY", "FIVESTAR", "FLUOROCHEM", "FORTIS", "FSL", "GAIL",
            "GESHIP", "GICRE", "GILLETTE", "GLAND", "GLAXO", "GLENMARK", "GMDCLTD",
            "GMRAIRPORT", "GNFC", "GODFRYPHLP", "GODIGIT", "GODREJAGRO", "GODREJCP",
            "GODREJIND", "GODREJPROP", "GPIL", "GPPL", "GRANULES", "GRAPHITE",
            "GRASIM", "GRAVITA", "GRSE", "GSPL", "GUJGASLTD", "GVT&D", "HAL",
            "HAPPSTMNDS", "HAVELLS", "HBLENGINE", "HCLTECH", "HDFCAMC", "HDFCBANK",
            "HDFCLIFE", "HEG", "HEROMOTOCO", "HFCL", "HINDALCO", "HINDCOPPER",
            "HINDPETRO", "HINDUNILVR", "HINDZINC", "HOMEFIRST", "HONASA", "HONAUT",
            "HSCL", "HUDCO", "HYUNDAI", "ICICIBANK", "ICICIGI", "ICICIPRULI", "IDBI",
            "IDEA", "IDFCFIRSTB", "IEX", "IFCI", "IGIL", "IGL", "IIFL", "IKS",
            "INDGN", "INDHOTEL", "INDIACEM", "INDIAMART", "INDIANB", "INDIGO",
            "INDUSINDBK", "INDUSTOWER", "INFY", "INOXINDIA", "INOXWIND", "INTELLECT",
            "IOB", "IOC", "IPCALAB", "IRB", "IRCON", "IRCTC", "IREDA", "IRFC", "ITC",
            "ITI", "J&KBANK", "JBCHEPHARM", "JBMA", "JINDALSAW", "JINDALSTEL",
            "JIOFIN", "JKCEMENT", "JKTYRE", "JMFINANCIL", "JPPOWER", "JSL",
            "JSWENERGY", "JSWHL", "JSWINFRA", "JSWSTEEL", "JUBLFOOD", "JUBLINGREA",
            "JUBLPHARMA", "JUSTDIAL", "JWL", "JYOTHYLAB", "JYOTICNC", "KAJARIACER",
            "KALYANKJIL", "KANSAINER", "KARURVYSYA", "KAYNES", "KEC", "KEI",
            "KFINTECH", "KIMS", "KIRLOSBROS", "KIRLOSENG", "KNRCON", "KOTAKBANK",
            "KPIL", "KPITTECH", "KPRMILL", "LALPATHLAB", "LATENTVIEW", "LAURUSLABS",
            "LEMONTREE", "LICHSGFIN", "LICI", "LINDEINDIA", "LLOYDSME", "LODHA",
            "LT", "LTF", "LTFOODS", "LTIM", "LTTS", "LUPIN", "M&M", "M&MFIN",
            "MAHABANK", "MAHSEAMLES", "MANAPPURAM", "MANKIND", "MANYAVAR",
            "MAPMYINDIA", "MARICO", "MARUTI", "MASTEK", "MAXHEALTH", "MAZDOCK",
            "MCX", "MEDANTA", "METROPOLIS", "MFSL", "MGL", "MINDACORP", "MMTC",
            "MOTHERSON", "MOTILALOFS", "MPHASIS", "MRF", "MRPL", "MSUMI",
            "MUTHOOTFIN", "NAM-INDIA", "NATCOPHARM", "NATIONALUM", "NAUKRI",
            "NAVA", "NAVINFLUOR", "NBCC", "NCC", "NESTLEIND", "NETWEB", "NETWORK18",
            "NEULANDLAB", "NEWGEN", "NH", "NHPC", "NIACL", "NIVABUPA", "NLCINDIA",
            "NMDC", "NSLNISP", "NTPC", "NTPCGREEN", "NUVAMA", "NYKAA", "OBEROIRLTY",
            "OFSS", "OIL", "OLAELEC", "OLECTRA", "ONGC", "PAGEIND", "PATANJALI",
            "PAYTM", "PCBL", "PEL", "PERSISTENT", "PETRONET", "PFC", "PFIZER",
            "PGEL", "PHOENIXLTD", "PIDILITIND", "PIIND", "PNB", "PNBHOUSING",
            "PNCINFRA", "POLICYBZR", "POLYCAB", "POLYMED", "POONAWALLA", "POWERGRID",
            "POWERINDIA", "PPLPHARMA", "PRAJIND", "PREMIERENE", "PRESTIGE", "PTCIL",
            "PVRINOX", "RADICO", "RAILTEL", "RAINBOW", "RAMCOCEM", "RAYMOND",
            "RAYMONDLSL", "RBLBANK", "RCF", "RECLTD", "REDINGTON", "RELIANCE",
            "RENUKA", "RHIM", "RITES", "RKFORGE", "ROUTE", "RPOWER", "RRKABEL",
            "RTNINDIA", "RVNL", "SAGILITY", "SAIL", "SAILIFE", "SAMMAANCAP",
            "SAPPHIRE", "SARDAEN", "SAREGAMA", "SBFC", "SBICARD", "SBILIFE",
            "SBIN", "SCHAEFFLER", "SCHNEIDER", "SCI", "SHREECEM", "SHRIRAMFIN",
            "SHYAMMETL", "SIEMENS", "SIGNATURE", "SJVN", "SKFINDIA", "SOBHA",
            "SOLARINDS", "SONACOMS", "SONATSOFTW", "SRF", "STARHEALTH", "SUMICHEM",
            "SUNDARMFIN", "SUNDRMFAST", "SUNPHARMA", "SUNTV", "SUPREMEIND", "SUZLON",
            "SWANCORP", "SWIGGY", "SWSOLAR", "SYNGENE", "SYRMA", "TANLA", "TARIL",
            "TATACHEM", "TATACOMM", "TATACONSUM", "TATAELXSI", "TATAINVEST",
            "TATAMOTORS", "TATAPOWER", "TATASTEEL", "TATATECH", "TBOTEK", "TCS",
            "TECHM", "TECHNOE", "TEJASNET", "THERMAX", "TIINDIA", "TIMKEN",
            "TITAGARH", "TITAN", "TORNTPHARM", "TORNTPOWER", "TRENT", "TRIDENT",
            "TRITURBINE", "TRIVENI", "TTML", "TVSMOTOR", "UBL", "UCOBANK",
            "ULTRACEMCO", "UNIONBANK", "UNITDSPR", "UNOMINDA", "UPL", "USHAMART",
            "UTIAMC", "VBL", "VEDL", "VGUARD", "VIJAYA", "VMM", "VOLTAS", "VTL",
            "WAAREEENER", "WELCORP", "WELSPUNLIV", "WESTLIFE", "WHIRLPOOL", "WIPRO",
            "WOCKPHARMA", "YESBANK", "ZEEL", "ZENSARTECH", "ZENTEC", "ZFCVINDIA",
            "ZYDUSLIFE", "AETHER"
        ]

    def init_database(self):
        """Initialize PostgreSQL database with proper schema."""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            # Create stocks table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stocks (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT UNIQUE NOT NULL,
                    name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            # Create stock_data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stock_data (
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
                    FOREIGN KEY (symbol) REFERENCES stocks (symbol)
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_data (symbol, date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON stock_data (symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON stock_data (date)')
            
            conn.commit()
            logger.info("Database initialized successfully")
            
        except psycopg2.Error as e:
            logger.error(f"Error initializing database: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def get_yahoo_symbol(self, symbol: str) -> str:
        """Convert Indian stock symbol to Yahoo Finance format."""
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

    def download_stock_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Download historical data for a single stock with retry logic."""
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
                
                # When auto_adjust=True, Close is already adjusted, so we duplicate it as adj_close
                data['adj_close'] = data['close']
                
                # Add symbol column
                data['symbol'] = symbol
                
                # Reorder columns
                data = data[['symbol', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
                
                logger.info(f"Successfully downloaded {len(data)} records for {symbol}")
                return data
                
            except Exception as e:
                logger.error(f"Error downloading {symbol} (attempt {attempt + 1}): {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to download {symbol} after {self.max_retries} attempts")
                    return None

    def save_stock_data(self, data: pd.DataFrame, symbol: str):
        """Save stock data to database using batch processing."""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            # Insert or update stock info
            cursor.execute('''
                INSERT INTO stocks (symbol, name)
                VALUES (%s, %s)
                ON CONFLICT (symbol) DO UPDATE SET name = EXCLUDED.name
            ''', (symbol, symbol))
            
            # Insert stock data in batches to avoid "too many SQL variables" error
            batch_size = 1000  # Process 1000 records at a time
            total_records = len(data)
            all_dates_to_check = []
            
            for i in range(0, total_records, batch_size):
                batch_data = data.iloc[i:i + batch_size]
                
                # Prepare data for batch insert
                records = []
                for _, row in batch_data.iterrows():
                    records.append((
                        row['symbol'],
                        row['date'],
                        float(row['open']) if pd.notna(row['open']) else None,
                        float(row['high']) if pd.notna(row['high']) else None,
                        float(row['low']) if pd.notna(row['low']) else None,
                        float(row['close']) if pd.notna(row['close']) else None,
                        float(row['adj_close']) if pd.notna(row['adj_close']) else None,
                        int(row['volume']) if pd.notna(row['volume']) else None
                    ))
                    all_dates_to_check.append(row['date'])
                
                # Insert batch using execute_batch for better performance
                execute_batch(cursor, '''
                    INSERT INTO stock_data 
                    (symbol, date, open, high, low, close, adj_close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, date) DO NOTHING
                ''', records, page_size=batch_size)
            
            conn.commit()
            
            # Verify how many records were actually inserted
            if all_dates_to_check:
                # Use ANY for PostgreSQL IN clause with list
                cursor.execute('''
                    SELECT COUNT(*) 
                    FROM stock_data 
                    WHERE symbol = %s AND date = ANY(%s)
                ''', (symbol, all_dates_to_check))
                
                inserted_count = cursor.fetchone()[0]
                logger.info(f"{symbol}: Attempted to save {total_records} records, verified {inserted_count} records in database")
                
                if inserted_count < total_records:
                    missing_count = total_records - inserted_count
                    logger.warning(f"{symbol}: Only {inserted_count} out of {total_records} records were saved. {missing_count} records may have been duplicates or failed.")
                else:
                    logger.info(f"{symbol}: Successfully saved all {total_records} records to database")
            
        except psycopg2.Error as e:
            logger.error(f"Error saving data for {symbol}: {str(e)}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def get_downloaded_symbols(self) -> set:
        """Get list of already downloaded symbols."""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT DISTINCT symbol FROM stock_data')
            symbols = {row[0] for row in cursor.fetchall()}
            return symbols
        except psycopg2.Error as e:
            logger.error(f"Error getting downloaded symbols: {e}")
            return set()
        finally:
            cursor.close()
            conn.close()

    def get_missing_dates_for_symbol(self, symbol: str, start_date: str, end_date: str) -> List[str]:
        """Get list of missing dates for a symbol within a date range."""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            # Get all dates that exist for this symbol
            cursor.execute('''
                SELECT DISTINCT date 
                FROM stock_data 
                WHERE symbol = %s AND date >= %s AND date <= %s
                ORDER BY date
            ''', (symbol, start_date, end_date))
            
            existing_dates = {row[0] for row in cursor.fetchall()}
            
            # Generate all trading dates in range (exclude weekends)
            date_range = pd.date_range(start=start_date, end=end_date, freq='B')  # 'B' = business days only
            all_dates = {date.date() for date in date_range}
            
            # Find missing dates
            missing_dates = sorted(all_dates - existing_dates)
            
            return [str(date) for date in missing_dates]
        except psycopg2.Error as e:
            logger.error(f"Error getting missing dates for {symbol}: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def get_last_date_for_symbol(self, symbol: str) -> Optional[str]:
        """Get the last (most recent) date for a symbol in the database."""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT MAX(date) 
                FROM stock_data 
                WHERE symbol = %s
            ''', (symbol,))
            
            result = cursor.fetchone()
            if result and result[0]:
                return result[0].strftime("%Y-%m-%d")
            return None
        except psycopg2.Error as e:
            logger.error(f"Error getting last date for {symbol}: {e}")
            return None
        finally:
            cursor.close()
            conn.close()

    def update_missing_dates(self, end_date: str = None):
        """Download missing dates for all symbols from their last date to end_date.
        
        Args:
            end_date: End date for checking missing dates. If None, uses today's date in IST.
        """
        if end_date is None:
            # Use today's date in IST timezone
            end_date = datetime.now(IST).strftime("%Y-%m-%d")
        
        logger.info(f"Updating missing dates for all symbols up to {end_date}")
        
        downloaded_symbols = self.get_downloaded_symbols()
        
        if not downloaded_symbols:
            logger.warning("No symbols found in database. Run download_all_stocks() first.")
            return
        
        logger.info(f"Checking {len(downloaded_symbols)} symbols for missing dates...")
        
        successful_updates = 0
        failed_updates = 0
        skipped_symbols = 0
        
        for symbol in sorted(downloaded_symbols):
            # Get the last date for this symbol
            last_date = self.get_last_date_for_symbol(symbol)
            
            if not last_date:
                logger.warning(f"{symbol}: No date found in database, skipping")
                skipped_symbols += 1
                continue
            
            # Calculate start date (last date + 1 day)
            last_date_obj = datetime.strptime(last_date, "%Y-%m-%d")
            start_date = (last_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            
            # Check if start_date is after end_date
            if start_date > end_date:
                logger.debug(f"{symbol}: Last date {last_date} is already >= end_date {end_date}, skipping")
                skipped_symbols += 1
                continue
            
            logger.info(f"{symbol}: Last date in DB: {last_date}, checking from {start_date} to {end_date}")
            
            # Get missing dates for this symbol
            missing_dates = self.get_missing_dates_for_symbol(symbol, start_date, end_date)
            
            if not missing_dates:
                logger.debug(f"{symbol}: No missing dates between {start_date} and {end_date}")
                skipped_symbols += 1
                continue
            
            logger.info(f"{symbol}: Found {len(missing_dates)} missing dates")
            
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
                        logger.warning(f"No data found for {symbol} in date range {start_date} to {end_date}")
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
                            data['adj_close'] = data['close']
                            data['symbol'] = symbol
                            data = data[['symbol', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
                            
                            self.save_stock_data(data, symbol)
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
        
        logger.info(f"Update completed! Successful: {successful_updates}, Failed: {failed_updates}, Skipped: {skipped_symbols}")

    def download_all_stocks(self):
        """Download data for all stocks with progress tracking."""
        total_stocks = len(self.stock_symbols)
        downloaded_symbols = self.get_downloaded_symbols()
        
        # Filter out already downloaded symbols
        remaining_symbols = [s for s in self.stock_symbols if s not in downloaded_symbols]
        
        logger.info(f"Total stocks: {total_stocks}")
        logger.info(f"Already downloaded: {len(downloaded_symbols)}")
        logger.info(f"Remaining to download: {len(remaining_symbols)}")
        
        if not remaining_symbols:
            logger.info("All stocks already downloaded!")
            return
        
        successful_downloads = 0
        failed_downloads = 0
        
        for i, symbol in enumerate(remaining_symbols, 1):
            logger.info(f"Progress: {i}/{len(remaining_symbols)} - Processing {symbol}")
            
            # Download data
            data = self.download_stock_data(symbol)
            
            if data is not None and not data.empty:
                # Save to database
                self.save_stock_data(data, symbol)
                successful_downloads += 1
            else:
                failed_downloads += 1
                logger.warning(f"Failed to download data for {symbol}")
            
            # Rate limiting
            if i < len(remaining_symbols):  # Don't delay after last request
                time.sleep(self.delay_between_requests)
        
        logger.info(f"Download completed!")
        logger.info(f"Successful downloads: {successful_downloads}")
        logger.info(f"Failed downloads: {failed_downloads}")

    def get_database_stats(self):
        """Get statistics about downloaded data."""
        conn = psycopg2.connect(self.db_url)
        cursor = conn.cursor()
        
        try:
            # Get total records
            cursor.execute('SELECT COUNT(*) FROM stock_data')
            total_records = cursor.fetchone()[0]
            
            # Get unique symbols
            cursor.execute('SELECT COUNT(DISTINCT symbol) FROM stock_data')
            unique_symbols = cursor.fetchone()[0]
            
            # Get date range
            cursor.execute('SELECT MIN(date), MAX(date) FROM stock_data')
            date_range = cursor.fetchone()
            
            # Get records per symbol
            cursor.execute('''
                SELECT symbol, COUNT(*) as record_count 
                FROM stock_data 
                GROUP BY symbol 
                ORDER BY record_count DESC 
                LIMIT 10
            ''')
            top_symbols = cursor.fetchall()
            
            logger.info("=== Database Statistics ===")
            logger.info(f"Total records: {total_records:,}")
            logger.info(f"Unique symbols: {unique_symbols}")
            logger.info(f"Date range: {date_range[0]} to {date_range[1]}")
            logger.info("Top 10 symbols by record count:")
            for symbol, count in top_symbols:
                logger.info(f"  {symbol}: {count:,} records")
        except psycopg2.Error as e:
            logger.error(f"Error getting database stats: {e}")
        finally:
            cursor.close()
            conn.close()

def main():
    """Main function to run the stock data downloader."""
    import sys
    
    downloader = StockDataDownloader()
    
    try:
        # Check if update flag is passed
        update_only = False
        if len(sys.argv) > 1 and sys.argv[1] == "--update":
            update_only = True
            logger.info("Updating missing dates...")
            # Update missing dates from each symbol's last date to end_date
            downloader.update_missing_dates(end_date=downloader.end_date)
        else:
            logger.info("Starting Stock Data Downloader")
            logger.info(f"Date range: {downloader.start_date} to {downloader.end_date}")
            
            # Download all stocks
            downloader.download_all_stocks()
            
            # Update any missing dates
            logger.info("Checking for missing dates...")
            downloader.update_missing_dates(end_date=downloader.end_date)
        
        # Show statistics
        downloader.get_database_stats()
        
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        logger.info("Stock data downloader finished")

if __name__ == "__main__":
    main()

