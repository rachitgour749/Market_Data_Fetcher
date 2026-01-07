#!/usr/bin/env python3
"""
Run all three data fetchers for a specific date range
Fetches data for US ETFs, Indian ETFs, and NIFTY 50 Index
"""

import logging
import sys
from datetime import datetime

# Import the downloaders
from us_etf_data import USEtfDownloader
from etf_data import ETFDataDownloader
from index_data import IndexDataDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('all_data_fetchers.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_all_fetchers(start_date: str, end_date: str):
    """
    Run all three data fetchers for the specified date range
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    """
    logger.info("="*80)
    logger.info(f"Starting data fetch for date range: {start_date} to {end_date}")
    logger.info("="*80)
    
    # Track results
    results = {
        'us_etf': {'success': False, 'error': None},
        'indian_etf': {'success': False, 'error': None},
        'nifty50': {'success': False, 'error': None}
    }
    
    # 1. Fetch US ETF Data
    logger.info("\n" + "="*80)
    logger.info("STEP 1/3: Fetching US ETF Data")
    logger.info("="*80)
    try:
        us_downloader = USEtfDownloader()
        # Override the end_date to match user's requirement
        us_downloader.end_date = end_date
        us_downloader.update_daily_data()
        results['us_etf']['success'] = True
        logger.info("✓ US ETF data fetch completed successfully")
    except Exception as e:
        logger.error(f"✗ Error fetching US ETF data: {str(e)}")
        results['us_etf']['error'] = str(e)
    
    # 2. Fetch Indian ETF Data
    logger.info("\n" + "="*80)
    logger.info("STEP 2/3: Fetching Indian ETF Data")
    logger.info("="*80)
    try:
        etf_downloader = ETFDataDownloader()
        etf_downloader.update_missing_dates(start_date=start_date, end_date=end_date)
        results['indian_etf']['success'] = True
        logger.info("✓ Indian ETF data fetch completed successfully")
    except Exception as e:
        logger.error(f"✗ Error fetching Indian ETF data: {str(e)}")
        results['indian_etf']['error'] = str(e)
    
    # 3. Fetch NIFTY 50 Index Data
    logger.info("\n" + "="*80)
    logger.info("STEP 3/3: Fetching NIFTY 50 Index Data")
    logger.info("="*80)
    try:
        index_downloader = IndexDataDownloader()
        index_downloader.update_missing_dates(start_date=start_date, end_date=end_date)
        results['nifty50']['success'] = True
        logger.info("✓ NIFTY 50 index data fetch completed successfully")
    except Exception as e:
        logger.error(f"✗ Error fetching NIFTY 50 data: {str(e)}")
        results['nifty50']['error'] = str(e)
    
    # Print summary
    logger.info("\n" + "="*80)
    logger.info("SUMMARY")
    logger.info("="*80)
    logger.info(f"Date Range: {start_date} to {end_date}")
    logger.info(f"US ETF Data: {'✓ SUCCESS' if results['us_etf']['success'] else '✗ FAILED'}")
    if results['us_etf']['error']:
        logger.info(f"  Error: {results['us_etf']['error']}")
    
    logger.info(f"Indian ETF Data: {'✓ SUCCESS' if results['indian_etf']['success'] else '✗ FAILED'}")
    if results['indian_etf']['error']:
        logger.info(f"  Error: {results['indian_etf']['error']}")
    
    logger.info(f"NIFTY 50 Index Data: {'✓ SUCCESS' if results['nifty50']['success'] else '✗ FAILED'}")
    if results['nifty50']['error']:
        logger.info(f"  Error: {results['nifty50']['error']}")
    
    logger.info("="*80)
    
    # Return success status
    all_success = all(r['success'] for r in results.values())
    return all_success

if __name__ == "__main__":
    # Default date range
    start_date = "2025-12-30"
    end_date = "2026-01-02"
    
    # Allow command line arguments to override
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    
    logger.info(f"Running all data fetchers for: {start_date} to {end_date}")
    
    try:
        success = run_all_fetchers(start_date, end_date)
        if success:
            logger.info("\n✓ All data fetchers completed successfully!")
            sys.exit(0)
        else:
            logger.warning("\n⚠ Some data fetchers failed. Check logs above for details.")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\n⚠ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Unexpected error: {str(e)}")
        sys.exit(1)
