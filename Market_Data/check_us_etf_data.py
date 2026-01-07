#!/usr/bin/env python3
"""
Test script to verify US ETF data in database
"""
import psycopg2
from datetime import datetime, timedelta

DATABASE_URL = "postgresql://neondb_owner:npg_WgVhOYtnP12l@ep-solitary-silence-a1yoj91r.ap-southeast-1.aws.neon.tech/MarketData?sslmode=require&channel_binding=require"

def check_data():
    """Check what data exists in international_etf_data table"""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    try:
        # Check total records
        cursor.execute("SELECT COUNT(*) FROM international_etf_data")
        total = cursor.fetchone()[0]
        print(f"\nTotal records in international_etf_data: {total:,}")
        
        # Check date range
        cursor.execute("SELECT MIN(date), MAX(date) FROM international_etf_data")
        min_date, max_date = cursor.fetchone()
        print(f"Date range: {min_date} to {max_date}")
        
        # Check number of unique symbols
        cursor.execute("SELECT COUNT(DISTINCT symbol) FROM international_etf_data")
        symbols = cursor.fetchone()[0]
        print(f"Unique symbols: {symbols}")
        
        # Check recent data (last 5 days)
        print("\n--- Recent Data (Last 5 Days) ---")
        cursor.execute("""
            SELECT date, COUNT(DISTINCT symbol) as symbol_count, COUNT(*) as record_count
            FROM international_etf_data
            WHERE date >= CURRENT_DATE - INTERVAL '5 days'
            GROUP BY date
            ORDER BY date DESC
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} symbols, {row[2]} records")
        
        # Check today's data specifically
        today = datetime.now().date()
        cursor.execute("""
            SELECT COUNT(*) FROM international_etf_data
            WHERE date = %s
        """, (today,))
        today_count = cursor.fetchone()[0]
        print(f"\nToday's data ({today}): {today_count} records")
        
        # Check yesterday's data
        yesterday = (datetime.now() - timedelta(days=1)).date()
        cursor.execute("""
            SELECT COUNT(*) FROM international_etf_data
            WHERE date = %s
        """, (yesterday,))
        yesterday_count = cursor.fetchone()[0]
        print(f"Yesterday's data ({yesterday}): {yesterday_count} records")
        
        # Sample data for a few symbols
        print("\n--- Sample Data (Last Date for Each Symbol) ---")
        cursor.execute("""
            SELECT symbol, MAX(date) as last_date, COUNT(*) as total_records
            FROM international_etf_data
            GROUP BY symbol
            ORDER BY symbol
            LIMIT 10
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]}: Last date = {row[1]}, Total records = {row[2]}")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_data()
