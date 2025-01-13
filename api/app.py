from fastapi import FastAPI, HTTPException
from redis import Redis
from cassandra.cluster import Cluster
from datetime import datetime
import json

app = FastAPI()
redis_client = Redis(host='localhost', port=6379)
CACHE_TTL = 3600  # 1 hour in seconds

@app.get("/sensors/{sensor_id}/latest")
async def get_latest_reading(sensor_id: str):
    # First try Redis
    cache_key = f"sensor:{sensor_id}:latest"
    cached = redis_client.get(cache_key)
    
    if cached:
        return json.loads(cached)
    
    # If not in cache, get from Cassandra
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect('sensor_keyspace')
        
        rows = session.execute(
            'SELECT * FROM sensor_aggregates WHERE sensor_id = %s ORDER BY window_start DESC LIMIT 1',
            [sensor_id]
        )
        
        row = rows.one()
        if not row:
            raise HTTPException(status_code=404, detail="Sensor data not found")
            
        # Format the response
        result = {
            'sensor_id': row.sensor_id,
            'window_start': row.window_start.isoformat(),
            'window_end': row.window_end.isoformat(),
            'avg_temp': float(row.avg_temp),
            'cached_at': datetime.now().isoformat()
        }
        
        # Cache the result
        redis_client.setex(cache_key, CACHE_TTL, json.dumps(result))
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

@app.get("/sensors/{sensor_id}/history")
async def get_sensor_history(sensor_id: str, limit: int = 10):
    # This endpoint bypasses cache and always gets from Cassandra
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect('sensor_keyspace')
        
        rows = session.execute(
            'SELECT * FROM sensor_aggregates WHERE sensor_id = %s ORDER BY window_start DESC LIMIT %s',
            [sensor_id, limit]
        )
        
        return [{
            'sensor_id': row.sensor_id,
            'window_start': row.window_start.isoformat(),
            'window_end': row.window_end.isoformat(),
            'avg_temp': float(row.avg_temp)
        } for row in rows]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

@app.delete("/sensors/{sensor_id}/cache")
async def clear_sensor_cache(sensor_id: str):
    # Utility endpoint to clear cache for testing
    cache_key = f"sensor:{sensor_id}:latest"
    redis_client.delete(cache_key)
    return {"message": f"Cache cleared for sensor {sensor_id}"} 