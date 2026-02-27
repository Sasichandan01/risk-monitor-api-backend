import psycopg2
from psycopg2.pool import SimpleConnectionPool
from config.ssm import config
import time
import threading
import logging

logger = logging.getLogger(__name__)

class DBPool:
    def __init__(self):
        self.call_pool = None
        self.put_pool = None
        self._init_pools()
        self.start_keep_alive() 

    def _init_pools(self):
        try:
            self.call_pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=config.NEON_CONNECTION_STRING_CALL
            )
            logger.info("Call DB pool connected")
        except psycopg2.Error as e:
            logger.error("Call DB connection failed: %s", str(e))
            raise

        try:
            self.put_pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=config.NEON_CONNECTION_STRING_PUT
            )
            logger.info("Put DB pool connected")
        except psycopg2.Error as e:
            logger.error("Put DB connection failed: %s", str(e))
            raise

    def get_call_conn(self):
        return self.call_pool.getconn()

    def get_put_conn(self):
        return self.put_pool.getconn()

    def return_call_conn(self, conn):
        self.call_pool.putconn(conn)

    def return_put_conn(self, conn):
        self.put_pool.putconn(conn)

    def close(self):
        if self.call_pool:
            self.call_pool.closeall()
        if self.put_pool:
            self.put_pool.closeall()

    def start_keep_alive(self):
        """Ping both DBs every 4 minutes to prevent Neon autosuspend"""
        def keep_alive():
            while True:
                time.sleep(240)  # 4 minutes
                for pool, label in [(self.call_pool, 'CE'), (self.put_pool, 'PE')]:
                    conn = None
                    try:
                        conn = pool.getconn()
                        cursor = conn.cursor()
                        cursor.execute("SELECT 1;")
                        cursor.close()
                    except Exception as e:
                        logger.warning("Keep-alive failed %s: %s", label, str(e))
                    finally:
                        if conn:
                            pool.putconn(conn)

        threading.Thread(target=keep_alive, daemon=True).start()
        logger.info("DB keep-alive started")

# Singleton
db = DBPool()
