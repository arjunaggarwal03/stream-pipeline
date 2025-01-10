CREATE KEYSPACE IF NOT EXISTS sensor_keyspace 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE sensor_keyspace;

CREATE TABLE IF NOT EXISTS sensor_aggregates (
    sensor_id text,
    window_start timestamp,
    window_end timestamp,
    avg_temp double,
    PRIMARY KEY (sensor_id, window_start)
); 