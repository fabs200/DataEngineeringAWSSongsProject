import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from logger import get_logger

# Initialize logger
logger = get_logger(__name__)

def load_staging_tables(cur, conn):
    for i, query in enumerate(copy_table_queries):
        try:
            logger.info(f"Loading staging table {i+1}/{len(copy_table_queries)}")
            cur.execute(query)
            conn.commit()
            logger.info(f"Successfully loaded staging table {i+1}")
        except Exception as e:
            logger.error(f"Error loading staging table: {e}")
            conn.rollback()
            raise

def insert_tables(cur, conn):
    for i, query in enumerate(insert_table_queries):
        try:
            logger.info(f"Inserting into table {i+1}/{len(insert_table_queries)}")
            cur.execute(query)
            conn.commit()
            logger.info(f"Successfully inserted into table {i+1}")
        except Exception as e:
            logger.error(f"Error inserting into table: {e}")
            conn.rollback()
            raise

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        logger.info("Starting ETL process")
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        logger.info("ETL process completed successfully")

    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()