import logging
import psycopg2
import os
from psycopg2 import sql, extras
from datetime import datetime, timezone, timedelta
import time

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

        

def fetch_and_create_tables(db_target_name):
    """Fetch all table names from the source database and create them in the target database."""
    conn_source = psycopg2.connect(dbname=db_source_name, user=db_user, password=db_password, host=db_host)
    conn_target = psycopg2.connect(dbname=db_target_name, user=db_user, password=db_password, host=db_host)
    try:
        cur_source = conn_source.cursor()
        cur_source.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = [table[0] for table in cur_source.fetchall()]
        cur_source.close()

        cur_target = conn_target.cursor()
        for table_name in tables:
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
            """).format(sql.Identifier(table_name.upper()))
            cur_target.execute(create_table_query)
        conn_target.commit()
        logging.info(f"Tables created successfully in {db_target_name}")
        return tables
    except Exception as e:
        logging.error(f"Error creating tables in {db_target_name}: {e}")
        return []
    finally:
        conn_source.close()
        conn_target.close()

def aggregate_and_insert_data(stock_table_name, sec, db_target_name):
    try:
        with psycopg2.connect(dbname=db_source_name, user=db_user, password=db_password, host=db_host) as conn_source:
            with conn_source.cursor() as cur_source:

                # Fetch the latest timestamp from the database
                cur_source.execute(sql.SQL("""
                    SELECT MAX(timestamp) 
                    FROM {}
                """).format(sql.Identifier(stock_table_name.upper())))

                latest_timestamp = cur_source.fetchone()[0]

                if not latest_timestamp:
                    return  # No data available yet

                current_time = latest_timestamp.replace(second=(latest_timestamp.second // sec) * sec, microsecond=0)
                previous_time = current_time - timedelta(seconds=sec)

                query = sql.SQL("""
                    SELECT timestamp, last_price, volume_traded
                    FROM {}
                    WHERE timestamp >= %s AND timestamp < %s
                    ORDER BY id ASC;
                """).format(sql.Identifier(stock_table_name.upper()))

                cur_source.execute(query, (previous_time, current_time))
                raw_data = cur_source.fetchall()

                if not raw_data:
                    return

                open_price = raw_data[0][1]
                close_price = raw_data[-1][1]
                high_price = max(row[1] for row in raw_data)
                low_price = min(row[1] for row in raw_data)
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




def main():


    # create_database("nifty_10sec", "postgres", "password")
    # create_database("nifty_20sec", "postgres", "password")
    # create_database("nifty_30sec", "postgres", "password")

    interval_seconds = int(input("Enter aggregation interval in seconds (10/20/30): "))
    db_target_name = f"nifty_{interval_seconds}sec"
    tables = fetch_and_create_tables(db_target_name)

    while True:
        # current_time = datetime.now(timezone.utc)
        # next_aggregate_time = (current_time.replace(second=(current_time.second // interval_seconds) * interval_seconds)
        #                        + timedelta(seconds=interval_seconds))

        for table_name in tables:
            aggregate_and_insert_data(table_name, interval_seconds, db_target_name)

        # Calculate sleep time until the next aggregation point
        # sleep_time = max(0, (next_aggregate_time - datetime.now(timezone.utc)).total_seconds())
        # logging.info(f"Sleeping for {sleep_time:.2f} seconds ")
        # time.sleep(sleep_time)


if __name__ == '__main__':
    main()

