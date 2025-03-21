import psycopg2
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

db_user = 'postgres'
db_password = 'anand1011'
db_host = 'localhost'

def truncate_tables(db_name):
    conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = [table[0] for table in cur.fetchall()]

            for table in tables:
                cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY;')
                conn.commit()  # Commit after each truncate to ensure consistency
                # logging.info(f"Table {table} truncated successfully.")

    except Exception as e:
        logging.error(f"Error while truncating tables: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    db_name = input("Enter the database name to truncate all tables: ").strip()
    truncate_tables(f"nifty_{db_name}")
    logging.info("Reset Successful")
