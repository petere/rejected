"""Automatically connect to a Redis database and provide a handle for using
it as self.redis.

"""
import logging
import redis

from rejected import consumer

LOGGER = logging.getLogger(__name__)


class RedisConsumer(consumer.BaseConsumer):
    """The RedisConsumer will automatically connect to Redis if there
    is a section in the consumer configuration entitled redis.

    Example config:

        Consumers:
          my_consumer:
            connections: [rabbitmq]
            consumer: rejected.example.Consumer
            max: 3
            queue: test_queue
            config:
              redis:
                host: localhost
                port: 6379
                db: 0

    """
    def initialize(self):
        """Run on initialization of the consumer. When extending this method
        be sure to invoke super(YourClass, self).initialize() or the
        initialization routine for this class will not be executed.

        """
        if 'redis' in self.config:
            self.redis = self._redis_client(self.config.get('host',
                                                            'localhost'),
                                            self.config.get('port', 6379),
                                            self.config.get('db', 0))
        super(RedisConsumer, self).initialize()

    def _redis_client(self, host, port, db):
        """Return a redis client for the given host, port and db.

        :param str host: The redis host to connect to
        :param int port: The redis port to connect to
        :param int db: The redis database number to use
        :rtype: redis.client.Redis

        """
        LOGGER.info('Returning a redis client connected to %s:%i:%i',
                    host, port, db)
        return redis.Redis(host=host, port=port, db=db)
