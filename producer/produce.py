import json
import time
import asyncio
import aiohttp
from typing import Dict, List
import logging
from datetime import datetime
from kafka import KafkaProducer
from urllib.parse import urlencode

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
EXCHANGES = {
    'binance': {
        'api_url': 'https://api.binance.us/api/v3/ticker/price',
        'symbols': ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
    },
    'coinbase': {
        'api_url': 'https://api.coinbase.com/v2/prices/{}/spot',
        'symbols': ['BTC-USD', 'ETH-USD']
    },
    'kraken': {
        'api_url': 'https://api.kraken.com/0/public/Ticker',
        'symbols': ['XBTUSDT', 'ETHUSDT']
    }
}

TRADING_PAIRS = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT']
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "crypto_prices"

class CryptoProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.session = None
        self.prices: Dict[str, Dict[str, float]] = {}

    async def setup(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()
        self.producer.close()

    async def fetch_binance_prices(self):
        try:
            # Properly encode the symbols parameter
            symbols = '[' + ','.join(f'"{symbol}"' for symbol in EXCHANGES['binance']['symbols']) + ']'
            # Properly encode URL parameters
            params = urlencode({'symbols': symbols})
            url = f"{EXCHANGES['binance']['api_url']}?{params}"
            
            async with self.session.get(url) as response:
                data = await response.json()
                if not isinstance(data, list):
                    logger.error(f"Unexpected Binance response format: {data}")
                    return
                
                # Convert list to dictionary for easier lookup
                price_dict = {}
                for item in data:
                    try:
                        if 'symbol' in item and 'price' in item:
                            price_dict[item['symbol']] = float(item['price'])
                    except (KeyError, ValueError) as e:
                        logger.error(f"Error parsing Binance price data: {item}, Error: {str(e)}")
                
                for symbol in EXCHANGES['binance']['symbols']:
                    if symbol in price_dict:
                        normalized_symbol = symbol.replace('USDT', '/USDT')
                        self.prices.setdefault('binance', {})[normalized_symbol] = price_dict[symbol]
                        logger.info(f"Binance {normalized_symbol}: {price_dict[symbol]}")
                    else:
                        logger.warning(f"Symbol {symbol} not found in Binance response")
        except Exception as e:
            logger.error(f"Error fetching Binance prices: {str(e)}", exc_info=True)

    async def fetch_coinbase_prices(self):
        try:
            for symbol in EXCHANGES['coinbase']['symbols']:
                url = EXCHANGES['coinbase']['api_url'].format(symbol)
                async with self.session.get(url) as response:
                    data = await response.json()
                    price = float(data['data']['amount'])
                    normalized_symbol = symbol.replace('-USD', '/USDT')
                    self.prices.setdefault('coinbase', {})[normalized_symbol] = price
                    logger.info(f"Coinbase {normalized_symbol}: {price}")
        except Exception as e:
            logger.error(f"Error fetching Coinbase prices: {str(e)}")

    async def fetch_kraken_prices(self):
        try:
            pair_params = ','.join(EXCHANGES['kraken']['symbols'])
            url = f"{EXCHANGES['kraken']['api_url']}?pair={pair_params}"
            
            async with self.session.get(url) as response:
                data = await response.json()
                if 'result' in data:
                    for pair, info in data['result'].items():
                        if 'XBT' in pair:
                            normalized_symbol = 'BTC/USDT'
                        else:
                            normalized_symbol = pair.replace('USDT', '/USDT')
                        self.prices.setdefault('kraken', {})[normalized_symbol] = float(info['c'][0])
                        logger.info(f"Kraken {normalized_symbol}: {float(info['c'][0])}")
        except Exception as e:
            logger.error(f"Error fetching Kraken prices: {str(e)}")

    def send_price_data(self):
        timestamp = datetime.utcnow().isoformat()
        
        for pair in TRADING_PAIRS:
            price_data = {
                'timestamp': timestamp,
                'trading_pair': pair,
                'prices': {
                    exchange: self.prices[exchange].get(pair)
                    for exchange in EXCHANGES.keys()
                    if exchange in self.prices and pair in self.prices[exchange]
                }
            }
            
            # Calculate price differences
            prices = [p for p in price_data['prices'].values() if p is not None]
            if len(prices) >= 2:
                price_data['max_diff'] = max(prices) - min(prices)
                price_data['max_diff_pct'] = (price_data['max_diff'] / min(prices)) * 100
                
                logger.info(f"Price differences for {pair}: {price_data['max_diff_pct']:.2f}%")
                self.producer.send(TOPIC_NAME, price_data)

    async def run(self):
        while True:
            try:
                # Fetch prices from all exchanges
                await asyncio.gather(
                    self.fetch_binance_prices(),
                    self.fetch_coinbase_prices(),
                    self.fetch_kraken_prices()
                )
                
                # Send data to Kafka
                self.send_price_data()
                
                # Wait before next fetch
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error in price fetching loop: {str(e)}")
                await asyncio.sleep(5)  # Wait longer on error

async def main():
    producer = CryptoProducer()
    await producer.setup()
    
    try:
        logger.info("Starting crypto price producer...")
        await producer.run()
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        await producer.close()

if __name__ == "__main__":
    asyncio.run(main())
