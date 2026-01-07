"""
Scheduler for running signal generation weekly
Runs every Friday at 5:00 PM IST
"""
import schedule
import time
from datetime import datetime
import pytz
from main import generate_etf_signals, generate_stock_signals

def job():
    """Run signal generation job"""
    print(f"\n{'='*60}")
    print(f"Starting Scheduled Signal Generation")
    print(f"Time: {datetime.now(pytz.timezone('Asia/Kolkata'))}")
    print(f"{'='*60}\n")
    
    try:
        # Generate signals for both ETF and Stock
        generate_etf_signals()
        generate_stock_signals()
        
        print(f"\n{'='*60}")
        print(f"Signal Generation Completed Successfully")
        print(f"{'='*60}\n")
    
    except Exception as e:
        print(f"\n❌ Error in signal generation: {e}")
        import traceback
        traceback.print_exc()

# Schedule job for every Friday at 5:00 PM IST
schedule.every().friday.at("17:00").do(job)

print("Signal Generation Scheduler Started")
print("Running every Friday at 5:00 PM IST")
print("Press Ctrl+C to stop\n")

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute
