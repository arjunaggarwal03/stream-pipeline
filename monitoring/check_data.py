from cassandra.cluster import Cluster
from datetime import datetime
import time

def check_cassandra():
    cluster = Cluster(['localhost'])
    session = cluster.connect('sensor_keyspace')
    
    while True:
        # First get distinct sensor IDs
        sensor_ids = session.execute('SELECT DISTINCT sensor_id FROM sensor_aggregates')
        
        print("\nLatest records at", datetime.now())
        print("sensor_id | window_start | avg_temp")
        print("-" * 40)
        
        # For each sensor, get its latest record
        for sensor in sensor_ids:
            sensor_id = sensor.sensor_id
            rows = session.execute(
                'SELECT * FROM sensor_aggregates WHERE sensor_id = %s ORDER BY window_start DESC LIMIT 1',
                [sensor_id]
            )
            for row in rows:
                print(f"{row.sensor_id} | {row.window_start} | {row.avg_temp:.2f}")
        time.sleep(10)

if __name__ == "__main__":
    check_cassandra() 