import requests
import json
import sqlite3
from datetime import datetime

def create_database():
    """
    Create SQLite database and table for HIP3 market data
    """
    conn = sqlite3.connect('hip3_markets.db')
    cursor = conn.cursor()
    
    # Create table with timestamp
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            dex TEXT NOT NULL,
            quote_asset TEXT NOT NULL,
            market TEXT NOT NULL,
            mark_price REAL,
            volume_24h REAL,
            open_interest REAL,
            funding_rate REAL
        )
    ''')
    
    # Create index for faster queries
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_market_timestamp 
        ON market_data(market, timestamp)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_dex_timestamp 
        ON market_data(dex, timestamp)
    ''')
    
    conn.commit()
    conn.close()
    print("✓ Database and table created successfully")

def insert_market_data(markets):
    """
    Insert market data into SQLite database
    """
    conn = sqlite3.connect('hip3_markets.db')
    cursor = conn.cursor()
    
    timestamp = datetime.now()
    
    for market in markets:
        cursor.execute('''
            INSERT INTO market_data 
            (timestamp, dex, quote_asset, market, mark_price, volume_24h, open_interest, funding_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            timestamp,
            market['dex'],
            market['quote'],
            market['market'],
            market['markPx'],
            market['dayNtlVlm'],
            market['openInterestUSD'],
            market['funding']
        ))
    
    conn.commit()
    rows_inserted = cursor.rowcount
    conn.close()
    
    return rows_inserted

def get_hip3_markets(dex="xyz", quote_currency="USDC"):
    """
    Get market data for a specific HIP3 dex
    """
    url = "https://api.hyperliquid.xyz/info"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    payload = {
        "type": "metaAndAssetCtxs",
        "dex": dex
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        data = response.json()
        
        metadata = data[0]
        asset_contexts = data[1]
        
        universe = metadata.get("universe", [])
        
        active_markets = []
        
        for i, market in enumerate(universe):
            coin_name = market.get("name", "N/A")
            market_data = asset_contexts[i]
            
            mark_px = market_data.get('markPx', '0')
            day_volume = market_data.get('dayNtlVlm', '0')
            open_interest_contracts = market_data.get('openInterest', '0')
            funding = market_data.get('funding', '0')
            
            try:
                mark_px_float = float(mark_px) if mark_px else 0
                oi_contracts_float = float(open_interest_contracts) if open_interest_contracts else 0
                day_volume_float = float(day_volume) if day_volume else 0
                funding_float = float(funding) if funding else 0
                
                oi_usd = oi_contracts_float * mark_px_float
                
                # Only include if volume > 0
                if day_volume_float > 0:
                    active_markets.append({
                        "dex": dex,
                        "quote": quote_currency,
                        "market": coin_name,
                        "markPx": mark_px_float,
                        "dayNtlVlm": day_volume_float,
                        "openInterestUSD": oi_usd,
                        "funding": funding_float
                    })
                    
            except (ValueError, TypeError):
                pass
        
        return active_markets
        
    except Exception as e:
        print(f"Error fetching {dex} markets: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_all_hip3_markets_combined():
    """
    Get market data from all HIP3 dex deployers
    """
    # Define dex configs with their quote currencies
    dex_configs = [
        {"name": "xyz", "quote": "USDC"},
        {"name": "flx", "quote": "USDH"},
        {"name": "vntl", "quote": "USDH"}
    ]
    
    all_markets = []
    
    for config in dex_configs:
        dex_name = config["name"]
        quote = config["quote"]
        print(f"\nFetching {dex_name} markets (quoted in {quote})...")
        markets = get_hip3_markets(dex_name, quote)
        all_markets.extend(markets)
        print(f"✓ Found {len(markets)} active markets in {dex_name}")
    
    # Sort by volume descending
    all_markets.sort(key=lambda x: -x['dayNtlVlm'])
    
    # Display all markets
    print(f"\n{'='*120}")
    print(f"All HIP3 Perpetual Markets (Sorted by 24h Volume)")
    print(f"{'='*120}")
    print(f"{'Rank':<6} {'Dex':<8} {'Quote':<8} {'Market':<20} {'Mark Price':<15} {'24h Volume':<20} {'Open Interest $':<20} {'Funding Rate':<15}")
    print(f"{'='*120}")
    
    for rank, market in enumerate(all_markets, 1):
        print(f"{rank:<6} {market['dex']:<8} {market['quote']:<8} {market['market']:<20} ${market['markPx']:<14.2f} ${market['dayNtlVlm']:<19,.0f} ${market['openInterestUSD']:<19,.0f} {market['funding']:<15.6f}")
    
    print(f"{'='*120}")
    # Overall totals
    total_volume = sum(m['dayNtlVlm'] for m in all_markets)
    total_oi = sum(m['openInterestUSD'] for m in all_markets)
    
    print(f"{'='*80}")
    print(f"Overall Totals:")
    print(f"  Total Active Markets: {len(all_markets)}")
    print(f"  Total 24h Volume: ${total_volume:,.2f}")
    print(f"  Total Open Interest: ${total_oi:,.2f}")

    # Summary by dex
    print(f"\nSummary by Dex:")
    print(f"{'='*80}")
    
    for config in dex_configs:
        dex_name = config["name"]
        quote = config["quote"]
        dex_markets = [m for m in all_markets if m['dex'] == dex_name]
        total_volume = sum(m['dayNtlVlm'] for m in dex_markets)
        total_oi = sum(m['openInterestUSD'] for m in dex_markets)
        
        print(f"{dex_name} (quoted in {quote}):")
        print(f"  Active Markets: {len(dex_markets)}")
        print(f"  Total 24h Volume: ${total_volume:,.2f}")
        print(f"  Total Open Interest: ${total_oi:,.2f}")
        print()
    

    
    return all_markets

def view_latest_data(limit=10):
    """
    View the latest data from the database
    """
    conn = sqlite3.connect('hip3_markets.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT timestamp, dex, quote_asset, market, mark_price, volume_24h, open_interest, funding_rate
        FROM market_data
        ORDER BY timestamp DESC, volume_24h DESC
        LIMIT ?
    ''', (limit,))
    
    rows = cursor.fetchall()
    conn.close()
    
    if rows:
        print(f"\n{'='*120}")
        print(f"Latest {limit} Market Records from Database")
        print(f"{'='*120}")
        print(f"{'Timestamp':<20} {'Dex':<8} {'Quote':<8} {'Market':<20} {'Mark Price':<15} {'24h Volume':<20} {'Open Interest':<20} {'Funding':<15}")
        print(f"{'='*120}")
        
        for row in rows:
            timestamp, dex, quote, market, mark_price, volume, oi, funding = row
            print(f"{timestamp:<20} {dex:<8} {quote:<8} {market:<20} ${mark_price:<14.4f} ${volume:<19,.2f} ${oi:<19,.2f} {funding:<15.6f}")
    else:
        print("No data in database yet")

if __name__ == "__main__":
    print("HIP3 Market Data Collector")
    print("=" * 80)
    
    # Create database
    create_database()
    
    # Fetch all market data
    markets_data = get_all_hip3_markets_combined()
    
    # Insert into database
    if markets_data:
        print(f"\n{'='*80}")
        print("Inserting data into database...")
        rows_inserted = insert_market_data(markets_data)
        print(f"✓ Inserted {rows_inserted} market records into database")
        
        # View latest data
        #view_latest_data(limit=20)
    else:
        print("No market data to insert")
