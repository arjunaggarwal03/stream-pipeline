from flask import Flask, request, jsonify
from cassandra.cluster import Cluster

app = Flask(__name__)

@app.route('/api/sensors/<sensor_id>/temperature')
def get_sensor_temp(sensor_id):
    cluster = Cluster(['localhost'])
    session = cluster.connect('sensor_keyspace')
    
    rows = session.execute(
        "SELECT * FROM sensor_aggregates WHERE sensor_id = %s ORDER BY window_start DESC LIMIT 1",
        [sensor_id]
    )
    return jsonify([dict(row) for row in rows]) 