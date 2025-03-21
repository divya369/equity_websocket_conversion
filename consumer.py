import redis
import orjson
import psycopg2
from psycopg2 import sql
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
redis_client = redis.Redis(host='localhost', port=6379, db=0)
stream_name = 'nifty_ticks'
consumer_group = 'tick_group'
deduplication_set = "processed_messages"
engine = create_engine('postgresql+psycopg2://postgres:anand1011@localhost:5432/nifty_trial', pool_size=1, max_overflow=0)
Session = sessionmaker(bind=engine)
session = Session()
directory_path = "logs"
os.makedirs(directory_path, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",handlers=[logging.FileHandler(f"{directory_path}/consumer_logs.log"),
                              logging.StreamHandler()])
instrument_df = pd.read_csv('filtered_kite_instruments.csv')
instrument_df = instrument_df[:20]
instrument_token_to_symbol = dict(zip(instrument_df['instrument_token'], instrument_df['tradingsymbol']))


def create_database(dbname, user, password, host="localhost", port="5432"):
    try:
        conn = psycopg2.connect(dbname="postgres", user=user, password=password, host=host, port=port)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Create the new database
        cur.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(dbname)))
        
        print(f"Database '{dbname}' created successfully.")
        
        # Close connection
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")


def save_to_postgresql(symbol, data):
    table_name = f"{symbol}"
    try:
        insert_query = text(f"""
            INSERT INTO "{table_name}"
                (timestamp, instrument_token, last_price, volume_traded)
            VALUES
                (:timestamp, :instrument_token, :last_price, :volume_traded)
        """)
        session.execute(insert_query, data)
        session.commit()
        logging.info(f"Inserted tick data for {symbol} at {data['timestamp']}")
        return True
    except Exception as e:
        session.rollback()
        logging.error(f"Error saving data to DB table {table_name}: {e}")
        return False


def process_message(msg_id, msg_value):
    """Process a single Redis message and acknowledge it only after successful insertion"""
    try:
        if redis_client.sismember(deduplication_set, msg_id):
            logging.info(f"Skipping duplicate message: {msg_id}")
            return

        serialized_data = msg_value.get(b'data', None)
        if serialized_data:
            data = orjson.loads(serialized_data)
            symbol = instrument_token_to_symbol.get(data['instrument_token'], str(data['instrument_token'])).upper()

            processed_data = {
                'timestamp': data.get("exchange_timestamp", " " ).replace("T", " "),
                'instrument_token': data.get("instrument_token"),
                'last_price': data.get("last_price"),
                'volume_traded': data.get("volume_traded")
            }

            if save_to_postgresql(symbol, processed_data):
                redis_client.xack(stream_name, consumer_group, msg_id)
                redis_client.sadd(deduplication_set, msg_id)
    except Exception as e:
        logging.error(f"Error processing message {msg_id}: {e}")


def redis_subscriber():
    """Single consumer for real-time processing"""
    logging.info(f"Listening for real-time tick data...")

    try:
        redis_client.xgroup_create(stream_name, consumer_group, id='$', mkstream=True)
    except redis.exceptions.ResponseError:
        pass

    while True:


        try:
            response = redis_client.xreadgroup(
                consumer_group, 'single_consumer', {stream_name: '>'}, count=200, block=0  # Read 1 at a time for real-time
            )


            if response:
                for stream, messages in response:
                    for msg_id, msg_value in messages:
                        process_message(msg_id, msg_value)


        except Exception as e:
            logging.error(f"Error in subscriber: {e}")


if __name__ == "__main__":
    """Run this to create new Database"""
    # create_database("nifty_trial", "postgres", "password")

    """Run this for Consumer Code"""
    redis_subscriber()
