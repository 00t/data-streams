import asyncio
import json
import os
from datetime import datetime
import pytz
from websockets import connect
from termcolor import cprint

# List of symbols you want to track
symbols = [
    'btcusdt', 
    'ethusdt', 
    'solusdt', 
    'bnbusdt', 
    'dogeusdt'
]
websocket_base_url = 'wss://stream.binance.us:9443'

# Define TradeAggregator class
class TradeAggregator:
    def __init__(self):
        self.trade_buckets = {}

    async def add_trade(self, symbol, timestamp, usd_size, is_buyer_maker):
        trade_key = (symbol, timestamp, is_buyer_maker)
        self.trade_buckets[trade_key] = self.trade_buckets.get(trade_key, 0) + usd_size
        
        # Print individual large trades immediately (lowered threshold)
        if usd_size > 5000:  # Show trades over $5,000
            trade_type = "BUY" if not is_buyer_maker else "SELL"
            back_color = 'on_blue' if not is_buyer_maker else 'on_magenta'
            display_size = usd_size / 1000  # Convert to thousands
            attrs = ['bold']
            if usd_size > 50000:  # Blink for very large trades
                attrs.append('blink')
            cprint(f"INSTANT {trade_type} {symbol} {timestamp} ${display_size:.2f}k", 'white', back_color, attrs=attrs)

    async def check_and_print_trades(self):
        timestamp_now = datetime.now(pytz.timezone('US/Eastern')).strftime("%H:%M:%S")
        deletions = []
        
        for trade_key, usd_size in self.trade_buckets.items():
            symbol, timestamp, is_buyer_maker = trade_key
            # Lowered threshold to $5,000 for aggregated trades
            if timestamp < timestamp_now and usd_size > 5000:  # Changed from 50000 to 5000
                trade_type = "BUY" if not is_buyer_maker else "SELL"
                back_color = 'on_blue' if not is_buyer_maker else 'on_magenta'

                display_size = usd_size / 1000  # Display in thousands
                attrs = ['bold']
                if usd_size > 50000:  # Blink for very large trades
                    attrs.append('blink')
                
                cprint(f"AGGR {trade_type} {symbol} {timestamp} ${display_size:.2f}k", 'white', back_color, attrs=attrs)
                deletions.append(trade_key)

        for key in deletions:
            del self.trade_buckets[key]

trade_aggregator = TradeAggregator()

# Define async function to handle trades
async def trade_handler(symbol, trade_aggregator):
    while True:
        try:
            uri = f"{websocket_base_url}/ws/{symbol}@aggTrade"
            async with connect(uri) as websocket:
                print(f"Connected to {symbol} stream")
                
                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)
                        
                        usd_size = float(data['p']) * float(data['q'])
                        trade_time = datetime.fromtimestamp(
                            data['T'] / 1000, 
                            pytz.timezone('US/Eastern')
                        ).strftime('%H:%M:%S')

                        # Determine trade type
                        trade_type = "SELL" if data['m'] else "BUY"
                        # Print trade details for debugging
                        print(f"{trade_type} {symbol} ${float(data['p']):.2f} - Size: {float(data['q']):.8f} - Total: ${usd_size:.2f}")

                        display_symbol = symbol.upper().replace('USDT', '')
                        await trade_aggregator.add_trade(
                            display_symbol,
                            trade_time, 
                            usd_size, 
                            data['m']
                        )

                    except Exception as e:
                        print(f"Error processing message for {symbol}: {e}")
                        await asyncio.sleep(1)
                        break

        except Exception as e:
            print(f"Connection error for {symbol}: {e}")
            await asyncio.sleep(5)
            continue

# Define function to print aggregated trades every second
async def print_aggregated_trades_every_second(aggregator):
    while True:
        await asyncio.sleep(1)
        await aggregator.check_and_print_trades()

# Define main function with graceful shutdown
async def main():
    try:
        tasks = [trade_handler(symbol, trade_aggregator) for symbol in symbols]
        tasks.append(print_aggregated_trades_every_second(trade_aggregator))
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    except Exception as e:
        print(f"Main loop error: {e}")
    finally:
        # Clean up code could go here
        pass

# Run the main function
asyncio.run(main())