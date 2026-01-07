
from etf_data import ETFDataDownloader
import logging
import sys

# Setup logging to see output
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def run_update():
    print("Initializing ETF Downloader...")
    downloader = ETFDataDownloader()
    
    start_date = "2025-12-30"
    end_date = "2026-01-02"
    
    print(f"Starting update for range: {start_date} to {end_date}")
    
    # Check if we should use download_all_etfs or update_missing_dates
    # The user asked to "fetch data between these dates"
    # update_missing_dates checks specifically for gaps in that range for existing symbols.
    
    downloader.update_missing_dates(start_date=start_date, end_date=end_date)
    print("Update process completed.")

if __name__ == "__main__":
    run_update()
