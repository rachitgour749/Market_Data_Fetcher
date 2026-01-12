import psycopg2
from datetime import datetime

DATABASE_URL = "postgresql://neondb_owner:npg_WgVhOYtnP12l@ep-solitary-silence-a1yoj91r.ap-southeast-1.aws.neon.tech/MarketData?sslmode=require&channel_binding=require"

def check_status():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    print("--- ETF Data (Indian) ---")
    cur.execute("SELECT symbol, date, created_at FROM etf_data ORDER BY created_at DESC LIMIT 5")
    for r in cur.fetchall():
        print(f"Symbol: {r[0]}, Date: {r[1]}, Created: {r[2]}")
        
    print("\n--- International ETF Data (US) ---")
    cur.execute("SELECT symbol, date, created_at FROM international_etf_data ORDER BY created_at DESC LIMIT 5")
    for r in cur.fetchall():
        print(f"Symbol: {r[0]}, Date: {r[1]}, Created: {r[2]}")
        
    print("\n--- Index Data (NIFTY50) ---")
    cur.execute("SELECT symbol, date, created_at FROM index_data ORDER BY created_at DESC LIMIT 5")
    for r in cur.fetchall():
        print(f"Symbol: {r[0]}, Date: {r[1]}, Created: {r[2]}")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_status()
