import redis
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def flush_redis_stream(stream_name):
    try:
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        response = redis_client.xtrim(stream_name, 0)  # Flushing by trimming to zero length
        logging.info(f"Successfully flushed stream '{stream_name}'. Entries removed: {response}")
    except Exception as e:
        logging.error(f"Error flushing stream '{stream_name}': {e}")

if __name__ == "__main__":
    stream_name = input("Enter the Redis stream name to flush: ").strip()
    flush_redis_stream(stream_name)
