
import psycopg2
import sys
from datetime import datetime

# Database connection string
DATABASE_URL = "postgresql://neondb_owner:npg_WgVhOYtnP12l@ep-solitary-silence-a1yoj91r.ap-southeast-1.aws.neon.tech/MarketData?sslmode=require&channel_binding=require"

def verify_data():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Check specific ETF data (SH - Short S&P 500)
        symbol = 'SH'
        print(f"Checking data for {symbol}...")
        cursor.execute("SELECT date, open, close FROM international_etf_data WHERE symbol = %s order by date desc limit 3", (symbol,))
        rows = cursor.fetchall()
        
        if rows:
            print(f"Found records for {symbol}:")
            for row in rows:
                print(f"Date: {row[0]}, Open: {row[1]}, Close: {row[2]}")
        else:
            print(f"No data found for {symbol}.")
            
        # Check specific ETF data (SQQQ - UltraPro Short QQQ)
        symbol = 'SQQQ'
        print(f"\nChecking data for {symbol}...")
        cursor.execute("SELECT date, open, close FROM international_etf_data WHERE symbol = %s order by date desc limit 3", (symbol,))
        rows = cursor.fetchall()
        
        if rows:
            print(f"Found records for {symbol}:")
            for row in rows:
                print(f"Date: {row[0]}, Open: {row[1]}, Close: {row[2]}")
        else:
            print(f"No data found for {symbol}.")
            
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_data()
