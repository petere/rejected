"""The Attribute Mixin assigns all aspects of the message to attributes
of the consumer class as attributes. This makes it so consumers can evaluate
attributes of the class (this.expiration, etc) as a shortcut for working
with the message data and properties.

"""
import datetime
import logging

from rejected import consumer

LOGGER = logging.getLogger(__name__)


class AttributeConsumer(consumer.BaseConsumer):
    """Dynamically inject attributes into the consumer to allow easy access to
    message properties and attributes of the pika.data.Message object.

    """
    INTEGER_PROPERTIES = ['delivery_mode', 'expiration', 'priority',
                          'timestamp']
    PROPERTIES = ['app_id', 'content_encoding', 'content_type',
                  'correlation_id', 'delivery_mode', 'expiration', 'headers',
                  'message_id', 'priority', 'reply_to', 'timestamp', 'type',
                  'user_id']
    MESSAGE_ATTRS = ['delivery_tag', 'exchange', 'routing_key', 'redelivered']

    def __init__(self, configuration):
        """Creates a new instance of a Consumer class. To perform
        initialization tasks, extend Consumer.initialize

        :param dict configuration: The configuration passed in from config
                file "config" section for the consumer

        """
        self.config = configuration
        self.delivery_tag = None
        self.app_id = None
        self.body = None
        self.channel = None
        self.content_encoding = None
        self.content_type = None
        self.correlation_id = None
        self.exchange = None
        self.expiration = None
        self.headers = None
        self.message = None
        self.message_id = None
        self.priority = None
        self.redelivered = None
        self.routing_key = None
        self.reply_to = None
        self.timestamp = None
        self.type = None
        self.user_id = None
        super(AttributeConsumer, self).__init__(configuration)

    def _receive(self, message):

        for name in self.PROPERTIES:
            if name in self.INTEGER_PROPERTIES:
                value = getattr(message.properties, name)
                setattr(self, name, int(value) if value is not None else None)
            else:
                setattr(self, name, getattr(message.properties, name))
        for name in self.MESSAGE_ATTRS:
            setattr(self, name, getattr(message, name))
        self.body = message.body
        super(AttributeConsumer, self)._receive(message)

    @property
    def datetime(self):
        """Return the datetime of the message from the properties.timestamp

        :rtype: datetime.datetime

        """
        if not self.message.properties.timestamp:
            return None
        float_value = float(self.message.properties.timestamp)
        return datetime.datetime.fromtimestamp(float_value)
