"""Automatically connect to a PgSQL database and provide a handle for using
it as self.cursor. The pgsql_wrapper class wraps psycopg2 and psycopg2_ctypes
so that both cpython and pypy can be supported.

"""
import logging
import pgsql_wrapper

from rejected import consumer

LOGGER = logging.getLogger(__name__)


class PgSQLConsumer(consumer.BaseConsumer):
    """The PgSQLConsumer will automatically connect to PostgreSQL if there
    is a "pgsql" section in the consumer configuration.

    Example config:

        Consumers:
          my_consumer:
            connections: [rabbitmq]
            consumer: example.Consumer
            max: 3
            queue: test_queue
            config:
              pgsql:
                host: localhost
                port: 5432
                dbname: postgres
                user: postgres
                password: optional

    """
    def initialize(self):
        """Run on initialization of the consumer. When extending this method
        be sure to invoke super(YourClass, self).initialize() or the
        initialization routine for this class will not be executed.

        """
        if 'pgsql' in self.config:
            config = self.config.get('pgsql')
            user = self.config.get('user', 'postgres')
            self.cursor = self._pgsql_cursor(config.get('host', 'localhost'),
                                             config.get('port', 5432),
                                             config.get('dbname', user),
                                             user,
                                             config.get('password', None))
        super(PgSQLConsumer, self).initialize()

    def _pgsql_cursor(self, host, port, dbname, user, password=None):
        """Connect to PostgreSQL and return the cursor.

        :param str host: The PostgreSQL host
        :param int port: The PostgreSQL port
        :param str dbname: The database name
        :param str user: The user name
        :param str password: The optional password
        :rtype: psycopg2.Cursor
        :raises: IOError

        """
        # Connect to PostgreSQL
        try:
            connection = pgsql_wrapper.PgSQL(host, port, dbname, user, password)
        except pgsql_wrapper.OperationalError as exception:
            raise IOError(exception)

        # Return a cursor
        return connection.cursor
