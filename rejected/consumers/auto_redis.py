
import logging
import redis

from rejected.consumers import base

LOGGER = logging.getLogger(__name__)


class RedisConnectedConsumer(base.Consumer):
    """The RedisConnectedConsumer will automatically connect to Redis if there
    is a section in the consumer configuration entitled redis.  Example config:

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
    def __init__(self, config):

        """Creates a new instance of a Consumer class. To perform
        initialization tasks, extend Consumer.initialize

        :param dict config: The configuration passed in from config file
                            "config" section for the consumer

        """
        if 'redis' in config:
            self.redis = self.get_redis_client(config.get('host', 'localhost'),
                                               config.get('port', 6379),
                                               config.get('db', 0))
        super(RedisConnectedConsumer, self).__init__(config)

    def get_redis_client(self, host, port, db):
        """Return a redis client for the given host, port and db.

        :param str host: The redis host to connect to
        :param int port: The redis port to connect to
        :param int db: The redis database number to use
        :rtype: redis.client.Redis

        """
        LOGGER.info('Returning a redis client connected to %s:%i:%i',
                    host, port, db)
        return redis.Redis(host=host, port=port, db=db)
