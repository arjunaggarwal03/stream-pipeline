from cassandra.cluster import Cluster
from datetime import datetime
import time
import json
from redis import Redis
from tabulate import tabulate

def check_data():
    cluster = Cluster(['localhost'])
    session = cluster.connect('crypto_keyspace')
    redis_client = Redis(host='localhost', port=6379)
    
    try:
        while True:
            print(f"\n=== Arbitrage Monitor at {datetime.now()} ===")
            
            # Get active trading pairs
            pairs = session.execute('SELECT DISTINCT trading_pair FROM arbitrage_opportunities')
            
            if not pairs:
                print("No trading pairs found")
                time.sleep(5)
                continue
            
            # For each pair, get latest opportunities
            opportunities = []
            for pair_row in pairs:
                pair = pair_row.trading_pair
                
                # Check Redis first for latest data
                redis_key_pattern = f"arbitrage:{pair}:*"
                redis_keys = redis_client.keys(redis_key_pattern)
                if redis_keys:
                    # Get latest from Redis
                    latest_key = sorted(redis_keys)[-1]
                    cached_data = redis_client.get(latest_key)
                    if cached_data:
                        opp = json.loads(cached_data)
                        opportunities.append([
                            pair,
                            opp['buy_exchange'],
                            opp['sell_exchange'],
                            f"${opp['buy_price']:.2f}",
                            f"${opp['sell_price']:.2f}",
                            f"{opp['profit_pct']:.2f}%",
                            datetime.fromisoformat(opp['timestamp']).strftime('%H:%M:%S')
                        ])
                else:
                    # Fallback to Cassandra
                    rows = session.execute(f"""
                        SELECT * FROM arbitrage_opportunities 
                        WHERE trading_pair = '{pair}' 
                        ORDER BY timestamp DESC LIMIT 1
                    """)
                    for row in rows:
                        opportunities.append([
                            row.trading_pair,
                            row.buy_exchange,
                            row.sell_exchange,
                            f"${row.buy_price:.2f}",
                            f"${row.sell_price:.2f}",
                            f"{row.profit_pct:.2f}%",
                            row.timestamp.strftime('%H:%M:%S')
                        ])
            
            if opportunities:
                print("\nCurrent Arbitrage Opportunities:")
                headers = ['Pair', 'Buy At', 'Sell At', 'Buy Price', 'Sell Price', 'Profit', 'Time']
                print(tabulate(opportunities, headers=headers, tablefmt='grid'))
                
                # Show summary stats
                total_opps = len(opportunities)
                avg_profit = sum(float(row[5].strip('%')) for row in opportunities) / total_opps
                print(f"\nSummary:")
                print(f"Total Opportunities: {total_opps}")
                print(f"Average Profit: {avg_profit:.2f}%")
            else:
                print("No current arbitrage opportunities")
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nStopping monitor...")
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    check_data() 