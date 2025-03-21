import redis
import orjson
import logging
import pandas as pd
import os
import time
from datetime import datetime, time
from kiteconnect import KiteTicker
import psycopg2

redis_client = redis.Redis(host='localhost', port=6379, db=0)
directory_path = "logs"
os.makedirs(directory_path, exist_ok=True)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(f"{directory_path}/producer_logger.log"),
                              logging.StreamHandler()])
db_connection = psycopg2.connect(dbname="nifty_trial",
    user="postgres",
    password="anand1011",
    host="localhost",
    port="5432")


cursor = db_connection.cursor()
instrument_df = pd.read_csv(f'filtered_kite_instruments.csv')
instrument_df = instrument_df[:10]
instrument_token_to_symbol = dict(zip(instrument_df['instrument_token'], instrument_df['tradingsymbol']))
tokens = instrument_df['instrument_token'].tolist()

def create_table(symbol):
    try:
        table_name = symbol.upper()
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP,
                instrument_token BIGINT,
                last_price FLOAT,
                volume_traded INT
            );
        """
        cursor.execute(create_table_query)
        db_connection.commit()
        logging.info(f"Table created for symbol: {symbol}")
    except Exception as e:
        logging.error(f"Error creating table for symbol {symbol}: {e}")

def on_ticks(ws, ticks):
    try:
        # Get the current time
        current_time = datetime.now().time()
        market_open = time(9, 15)
        market_close = time(15, 30)

        # Process ticks only during market hours
        if market_open <= current_time <= market_close:
            logging.info(f"Received {len(ticks)} ticks.")
            # logging.info(f"Tick Data: {(ticks)}")  # Logging received tick data
            # time.sleep(10)
            for tick in ticks:
                serialized_tick = orjson.dumps(tick)
                redis_client.xadd('nifty_ticks', {'data': serialized_tick}, maxlen=10000, approximate=False)
        else:
            logging.info("Market is closed. Ignoring incoming ticks.")
            return 

    except Exception as e:
        logging.error(f"Error processing ticks: {e}")

def on_connect(ws, response):
    logging.info(f"WebSocket connected. Subscribing to {len(tokens)} tokens.")
    ws.subscribe(tokens)
    ws.set_mode(KiteTicker.MODE_FULL, tokens)


def on_close(ws, code, reason):
    logging.info(f"Connection closed: {code}, {reason}. Reconnecting...")
    time.sleep(1)
    kws.connect()


def on_error(ws, error, reason):
    logging.error(f"WebSocket error: {error}. Reason: {reason}")
    reconnect(ws)


def reconnect(ws):
        delay = 1  
        max_attempts = 3
        for attempt in range(max_attempts):
            print(f"Attempting to reconnect... (Attempt {attempt + 1}/{max_attempts})")
            try:
                ws.connect(threaded=True)
                print("Reconnected successfully!")
                return
            except Exception as e:
                print(f"Reconnection failed: {e}")
                time.sleep(delay)
        logging.info("Max reconnection attempts reached. Exiting.")


def main():
    try:
        for token in tokens:
            symbol = instrument_token_to_symbol.get(token, f"Token_{token}").upper()
            """Run this Create Table Function when the tables are missing, don't run otherwise"""
            create_table(symbol)
        kws.connect()
    except Exception as e:
        logging.error(f"Error connecting to KiteTicker: {e}. Retrying...")
        main()


if __name__ == "__main__":
    kws = KiteTicker("et1cdt1p2tlliq99", "DhgexoqFWnwpSbk6CI9oHGplBHKMg0Lg")
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    main()