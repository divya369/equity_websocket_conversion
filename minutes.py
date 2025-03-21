import psycopg2
from psycopg2 import sql, extras
from datetime import datetime, timezone, timedelta
import os
import logging
import time

# from redis_nifty.FINAL_raw.config import db_target_name

db_user = 'postgres'
db_password = 'anand1011'
db_host = 'localhost'
db_source_name = 'nifty_trial'

# Logging configuration
DIRECTORY_PATH = "logs"
os.makedirs(DIRECTORY_PATH, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{DIRECTORY_PATH}/Conversion_Log.log"),
        logging.StreamHandler()
    ]
)


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


def fetch_tables(db_target_name):
    """Fetches all tables from the source database and ensures they exist in the target database."""
    conn_source = psycopg2.connect(dbname=db_source_name, user=db_user, password=db_password, host=db_host)
    conn_target = psycopg2.connect(dbname=db_target_name, user=db_user, password=db_password, host=db_host)

    try:
        cur_source = conn_source.cursor()
        cur_source.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = [table[0] for table in cur_source.fetchall()]
        cur_source.close()

        cur_target = conn_target.cursor()
        for stock_table_name in tables:
            create_table_query = sql.SQL(""" 
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL UNIQUE,
                    timestamp TIMESTAMP PRIMARY KEY,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume_traded BIGINT
                );
            """).format(sql.Identifier(stock_table_name.upper()))
            cur_target.execute(create_table_query)

        conn_target.commit()
        cur_target.close()

        return tables

    finally:
        conn_source.close()
        conn_target.close()


def aggregate_and_insert_data(stock_table_name, interval_minutes, db_target_name):
    try:
        with psycopg2.connect(dbname=db_source_name, user=db_user, password=db_password, host=db_host) as conn_source:
            with conn_source.cursor() as cur_source:
                cur_source.execute(sql.SQL("""
                    SELECT MAX(timestamp) 
                    FROM {}
                """).format(sql.Identifier(stock_table_name.upper())))
                latest_timestamp = cur_source.fetchone()[0]
                if not latest_timestamp:
                    # logging.info("Error Getting Latest Timestamp from Source DB...")
                    return

                current_time = latest_timestamp.replace(second=0, microsecond=0)

                if interval_minutes == 1:
                    last_time = current_time
                    previous_time = current_time - timedelta(minutes=1)
                elif interval_minutes == 2:
                    minutes_since_start = (current_time.hour * 60 + current_time.minute) - 9 * 60 - 15
                    last_time = current_time - timedelta(minutes=minutes_since_start % 2)
                    previous_time = last_time - timedelta(minutes=2)
                elif interval_minutes == 5:
                    last_time = current_time - timedelta(minutes=current_time.minute % 5)
                    previous_time = last_time - timedelta(minutes=5)
                elif interval_minutes == 10:
                    minutes_since_start = (current_time.hour * 60 + current_time.minute) - 9 * 60 - 15
                    last_time = current_time - timedelta(minutes=minutes_since_start % 10)
                    if last_time.minute % 10 != 5:  # Ensure alignment to 9:15, 9:25, etc.
                        last_time -= timedelta(minutes=last_time.minute % 10 - 5)
                    previous_time = last_time - timedelta(minutes=10)
                elif interval_minutes == 15:
                    last_time = current_time - timedelta(minutes=current_time.minute % 15)
                    previous_time = last_time - timedelta(minutes=15)
                elif interval_minutes == 30:
                    minutes_since_start = (current_time.hour * 60 + current_time.minute) - 9 * 60 - 15
                    last_time = current_time - timedelta(minutes=minutes_since_start % 30)
                    previous_time = last_time - timedelta(minutes=30)
                elif interval_minutes == 45:
                    minutes_since_start = (current_time.hour * 60 + current_time.minute) - 9 * 60 - 15
                    last_time = current_time - timedelta(minutes=minutes_since_start % 45)
                    previous_time = last_time - timedelta(minutes=45)
                elif interval_minutes == 75:
                    minutes_since_start = (current_time.hour * 60 + current_time.minute) - 9 * 60 - 15
                    last_time = current_time - timedelta(minutes=minutes_since_start % 75)
                    previous_time = last_time - timedelta(minutes=75)

                query = sql.SQL("""
                    SELECT timestamp, last_price, volume_traded
                    FROM {}
                    WHERE timestamp >= %s AND timestamp < %s
                    ORDER BY timestamp ASC;
                """).format(sql.Identifier(stock_table_name.upper()))

                cur_source.execute(query, (previous_time, last_time))
                raw_data = cur_source.fetchall()

                if not raw_data:
                    logging.warning(
                        f"No data to aggregate for {stock_table_name.upper()} during {previous_time} to {last_time}.")
                    return

                open_price = raw_data[0][1]
                high_price = max(row[1] for row in raw_data)
                low_price = min(row[1] for row in raw_data)
                close_price = raw_data[-1][1]
                volume_traded = sum(row[2] for row in raw_data)

                aggregated_data = [(previous_time, open_price, high_price, low_price, close_price, volume_traded)]

    except Exception as e:
        print(f"Error aggregating OHLC data for {stock_table_name.upper()}: {e}")
        return



    try:
        with psycopg2.connect(dbname=db_target_name, user=db_user, password=db_password, host=db_host) as conn_target:
            with conn_target.cursor() as cur_target:
                cur_target.execute(sql.SQL("LOCK TABLE {} IN EXCLUSIVE MODE;").format(sql.Identifier(stock_table_name.upper())))

                insert_query = sql.SQL("""
                    INSERT INTO {} (timestamp, open, high, low, close, volume_traded)
                    VALUES %s
                    ON CONFLICT (timestamp) DO UPDATE
                    SET open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume_traded = EXCLUDED.volume_traded;
                """).format(sql.Identifier(stock_table_name.upper()))

                extras.execute_values(cur_target, insert_query, aggregated_data)

                cur_target.execute(sql.SQL("""
                    SELECT setval(pg_get_serial_sequence('{}', 'id'), COALESCE(MAX(id), 1), true)
                    FROM {};
                """).format(sql.Identifier(stock_table_name.upper()), sql.Identifier(stock_table_name.upper())))

                logging.info(f"Inserted Data for {stock_table_name.upper()}")
                conn_target.commit()

    except Exception as e:
        print(f"Error inserting data for {stock_table_name.upper()}: {e}")


if __name__ == '__main__':

    # """Create Databases"""
    # create_database("nifty_1min", "postgres", "password")
    # create_database("nifty_2min", "postgres", "password")
    # create_database("nifty_5min", "postgres", "password")
    # create_database("nifty_10min", "postgres", "password")
    # create_database("nifty_15min", "postgres", "password")
    # create_database("nifty_30min", "postgres", "password")
    # create_database("nifty_45min", "postgres", "password")
    # create_database("nifty_75min", "postgres", "password")


    interval_minutes = int(input("Enter aggregation interval in minutes \n(1/2/5/10/15/30/45/75) : "))
    while True:
        db_target_name = f'nifty_{interval_minutes}min'
        stock_tables = fetch_tables(db_target_name)
        for stock_table_name in stock_tables:
                    aggregate_and_insert_data(stock_table_name, interval_minutes, db_target_name)

