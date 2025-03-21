import psycopg2
from psycopg2 import sql, extras
from datetime import datetime, timezone, timedelta
import time
import os
import logging
db_user = 'postgres'
db_password = 'anand1011'
db_host = 'localhost'
db_source_name = 'nifty_trial'
directory_path = "logs"
os.makedirs(directory_path, exist_ok=True)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(f"{directory_path}/Conversion_Log.log"),
                              logging.StreamHandler()])



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


def aggregate_and_insert_data(stock_table_name, interval_hours, db_target_name):
    conn_source = psycopg2.connect(dbname=db_source_name, user=db_user, password=db_password, host=db_host)
    cur_source = conn_source.cursor()

    try:
        # Wait until data becomes available
        while True:
            cur_source.execute(sql.SQL("""
                SELECT MAX(timestamp)
                FROM {}
            """).format(sql.Identifier(stock_table_name.upper())))
            latest_timestamp = cur_source.fetchone()[0]

            if latest_timestamp:
                break  # Data found — proceed with aggregation
            else:
                logging.warning(f"No data yet in {stock_table_name.upper()}. Waiting for new data...")
                time.sleep(10)  # Wait 10 seconds before retrying

        # Align to the nearest HH:15
        current_time = latest_timestamp.replace(second=0, microsecond=0)
        checkpoint_minute = 15
        aligned_hour = current_time.hour - (current_time.hour % interval_hours)
        last_checkpoint = current_time.replace(hour=aligned_hour, minute=checkpoint_minute, second=0, microsecond=0)
        previous_checkpoint = last_checkpoint - timedelta(hours=interval_hours)

        # Ensure the hour has completed before aggregation
        if current_time < last_checkpoint + timedelta(hours=interval_hours):
            logging.info(f"Skipping aggregation — interval not yet completed. Next checkpoint: {last_checkpoint + timedelta(hours=interval_hours)}")
            return

        # Fetch data between the checkpoints
        query = sql.SQL("""
            SELECT timestamp, last_price, volume_traded
            FROM {}
            WHERE timestamp >= %s AND timestamp < %s
            ORDER BY timestamp ASC;
        """).format(sql.Identifier(stock_table_name.upper()))

        cur_source.execute(query, (previous_checkpoint, last_checkpoint))
        raw_data = cur_source.fetchall()

        if not raw_data:
            logging.warning(f"No data to aggregate for {stock_table_name.upper()} during {previous_checkpoint} to {last_checkpoint}.")
            return

        # Aggregation logic
        open_price = raw_data[0][1]
        high_price = max(row[1] for row in raw_data)
        low_price = min(row[1] for row in raw_data)
        close_price = raw_data[-1][1]
        volume_traded = sum(row[2] for row in raw_data)

        aggregated_data = [(previous_checkpoint, open_price, high_price, low_price, close_price, volume_traded)]

    finally:
        cur_source.close()
        conn_source.close()

    if not aggregated_data:
        return

    # Insert aggregated data into the target database
    conn_target = psycopg2.connect(dbname=db_target_name, user=db_user, password=db_password, host=db_host)
    cur_target = conn_target.cursor()

    try:
        cur_target.execute(sql.SQL("LOCK TABLE {} IN EXCLUSIVE MODE;").format(sql.Identifier(stock_table_name.upper())))

        insert_query = sql.SQL("""
            INSERT INTO {} (timestamp, open, high, low, close, volume_traded)
            VALUES %s
            ON CONFLICT (timestamp) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume_traded = EXCLUDED.volume_traded
        """).format(sql.Identifier(stock_table_name.upper()))

        extras.execute_values(cur_target, insert_query, aggregated_data)

        # Reset ID sequence
        cur_target.execute(sql.SQL("""
            SELECT setval(pg_get_serial_sequence('{}', 'id'), COALESCE(MAX(id), 1), true)
            FROM {};
        """).format(sql.Identifier(stock_table_name.upper()), sql.Identifier(stock_table_name.upper())))

        conn_target.commit()

        logging.info(f"Aggregated {interval_hours}-hour data inserted into {stock_table_name.upper()} successfully.")

    finally:
        cur_target.close()
        conn_target.close()


def process_tables(interval_hour):
    while True:
        db_target_name = f'nifty_{interval_hour}hr'
        stock_tables = fetch_tables(db_target_name)
        for stock_table_name in stock_tables:
            aggregate_and_insert_data(stock_table_name, interval_hour, db_target_name)

if __name__ == '__main__':

    """Run this for Database Creation"""

    create_database("nifty_3hr", "postgres", "password")
    create_database("nifty_1hr", "postgres", "password")
    create_database("nifty_2hr", "postgres", "password")


    """Now run this for aggregation"""
    # interval_hour = int(input("Enter aggregation interval in Hours \n(1/2/3) : "))
    # process_tables(interval_hour)
