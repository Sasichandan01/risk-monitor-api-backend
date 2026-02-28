import psycopg2
from psycopg2.pool import SimpleConnectionPool
from config.ssm import config
import time
import threading
import logging

logger = logging.getLogger(__name__)

class DBPool:
    def __init__(self):
        """
        Initializes a DBPool instance.

        Establishes two connection pools: one for the calling database and one for the putting database.
        Starts a keep-alive thread that pings the databases every 4 minutes to prevent idle connections from being closed by the database server.

        Attributes:
            call_pool (SimpleConnectionPool): Connection pool for the calling database
            put_pool (SimpleConnectionPool): Connection pool for the putting database
        """
        self.call_pool = None
        self.put_pool = None
        self._init_pools()
        self.start_keep_alive() 

    def _init_pools(self):
        """
        Initializes the connection pools for the calling and putting databases.

        Establishes two connection pools using the connection strings from the configuration.

        If either connection pool initialization fails, logs an error message with the error details and raises an exception.

        Logs an info message when each connection pool is successfully connected.
        """
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
        """
        Returns a connection from the calling database connection pool.

        Returns:
            conn (psycopg2.extensions.connection): A database connection object
        """
        return self.call_pool.getconn()

    def get_put_conn(self):
        """
        Returns a connection from the putting database connection pool.

        Returns:
            conn (psycopg2.extensions.connection): A database connection object
        """
        return self.put_pool.getconn()

    def return_call_conn(self, conn):
        """
        Returns a connection to the calling database connection pool.

        Parameters:
            conn (psycopg2.extensions.connection): A database connection object

        Returns:
            None
        """
        self.call_pool.putconn(conn)

    def return_put_conn(self, conn):
        """
        Returns a connection to the putting database connection pool.

        Parameters:
            conn (psycopg2.extensions.connection): A database connection object

        Returns:
            None
        """
        self.put_pool.putconn(conn)

    def close(self):
        """
        Closes all database connections in the calling and putting connection pools.

        If the connection pools are not None, it closes all connections in each pool.
        """
        if self.call_pool:
            self.call_pool.closeall()
        if self.put_pool:
            self.put_pool.closeall()

    def start_keep_alive(self):
        """Ping both DBs every 4 minutes to prevent Neon autosuspend"""
        def keep_alive():
            """
            Pings both DBs every 4 minutes to prevent Neon autosuspend.

            This function runs in an infinite loop, sleeping for 4 minutes between each iteration.
            On each iteration, it gets a connection from each pool (CE and PE), executes a SELECT 1 query, and then puts the connection back into the pool.
            If an exception occurs during the query execution, it logs a warning message with the error details.
            """
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
