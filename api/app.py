from fastapi import FastAPI, HTTPException, Query
from redis import Redis
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from typing import List, Optional
import json

app = FastAPI(title="Crypto Arbitrage API")
redis_client = Redis(host='localhost', port=6379)

# Initialize Cassandra connection
cluster = Cluster(['localhost'])
session = cluster.connect('crypto_keyspace')

@app.get("/opportunities/current")
async def get_current_opportunities(
    min_profit: float = Query(0.5, description="Minimum profit percentage"),
    trading_pair: Optional[str] = Query(None, description="Specific trading pair to query")
):
    """Get current arbitrage opportunities."""
    try:
        opportunities = []
        
        # Build Redis key pattern
        key_pattern = f"arbitrage:{trading_pair or '*'}:*"
        redis_keys = redis_client.keys(key_pattern)
        
        for key in sorted(redis_keys, reverse=True):
            data = redis_client.get(key)
            if data:
                opp = json.loads(data)
                if opp['profit_pct'] >= min_profit:
                    opportunities.append(opp)
        
        return {
            "count": len(opportunities),
            "opportunities": opportunities
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/opportunities/history/{trading_pair}")
async def get_opportunity_history(
    trading_pair: str,
    hours: int = Query(1, description="Hours of history to retrieve"),
    min_profit: float = Query(0.0, description="Minimum profit percentage filter")
):
    """Get historical arbitrage opportunities for a specific trading pair."""
    try:
        # Query Cassandra for historical data
        query = """
            SELECT * FROM arbitrage_opportunities 
            WHERE trading_pair = %s 
            AND timestamp > %s
            AND profit_pct >= %s
            ALLOW FILTERING
        """
        
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        rows = session.execute(query, (trading_pair, cutoff_time, min_profit))
        
        opportunities = [{
            'timestamp': row.timestamp.isoformat(),
            'trading_pair': row.trading_pair,
            'buy_exchange': row.buy_exchange,
            'sell_exchange': row.sell_exchange,
            'buy_price': row.buy_price,
            'sell_price': row.sell_price,
            'profit_pct': row.profit_pct,
            'volume_limit': row.volume_limit
        } for row in rows]
        
        return {
            "trading_pair": trading_pair,
            "period_hours": hours,
            "count": len(opportunities),
            "opportunities": opportunities
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/exchanges/performance")
async def get_exchange_performance(
    hours: int = Query(24, description="Hours of history to analyze")
):
    """Get exchange performance metrics."""
    try:
        query = """
            SELECT buy_exchange, sell_exchange, profit_pct 
            FROM arbitrage_opportunities 
            WHERE timestamp > %s
            ALLOW FILTERING
        """
        
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        rows = session.execute(query, (cutoff_time,))
        
        exchange_stats = {}
        for row in rows:
            # Track buy-side performance
            if row.buy_exchange not in exchange_stats:
                exchange_stats[row.buy_exchange] = {
                    'buy_count': 0,
                    'sell_count': 0,
                    'total_profit_buy': 0,
                    'total_profit_sell': 0
                }
            exchange_stats[row.buy_exchange]['buy_count'] += 1
            exchange_stats[row.buy_exchange]['total_profit_buy'] += row.profit_pct
            
            # Track sell-side performance
            if row.sell_exchange not in exchange_stats:
                exchange_stats[row.sell_exchange] = {
                    'buy_count': 0,
                    'sell_count': 0,
                    'total_profit_buy': 0,
                    'total_profit_sell': 0
                }
            exchange_stats[row.sell_exchange]['sell_count'] += 1
            exchange_stats[row.sell_exchange]['total_profit_sell'] += row.profit_pct
        
        # Calculate averages
        for exchange in exchange_stats:
            stats = exchange_stats[exchange]
            stats['avg_profit_buy'] = stats['total_profit_buy'] / stats['buy_count'] if stats['buy_count'] > 0 else 0
            stats['avg_profit_sell'] = stats['total_profit_sell'] / stats['sell_count'] if stats['sell_count'] > 0 else 0
        
        return {
            "period_hours": hours,
            "exchange_stats": exchange_stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Check system health."""
    try:
        # Check Redis
        redis_ok = redis_client.ping()
        
        # Check Cassandra
        cassandra_ok = session.execute("SELECT now() FROM system.local").one() is not None
        
        return {
            "status": "healthy" if redis_ok and cassandra_ok else "unhealthy",
            "redis": "connected" if redis_ok else "disconnected",
            "cassandra": "connected" if cassandra_ok else "disconnected",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 