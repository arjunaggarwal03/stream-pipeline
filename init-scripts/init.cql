CREATE KEYSPACE IF NOT EXISTS crypto_keyspace 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE crypto_keyspace;

DROP TABLE IF EXISTS arbitrage_opportunities;

CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    timestamp timestamp,
    trading_pair text,
    buy_exchange text,
    sell_exchange text,
    buy_price double,
    sell_price double,
    profit_pct double,
    volume_limit double,
    PRIMARY KEY ((trading_pair), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Add index for faster querying
CREATE INDEX IF NOT EXISTS idx_timestamp ON arbitrage_opportunities(timestamp); 