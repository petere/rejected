"""
The Attribute Consumer assigns all aspects of the message to attributes of the
consumer class.

"""
import copy
import datetime
import logging

LOGGER = logging.getLogger(__name__)

from rejected.consumers import base


class AttributeConsumer(base.Consumer):
    """Dynamically inject attributes into the consumer to allow easy access to
    message properties and attributes of the pika.data.Message object.

    """
    _INTEGER_PROPERTIES = ['expiration', 'priority', 'timestamp']
    _PROPERTIES = ['app_id', 'content_encoding', 'content_type',
                   'correlation_id', 'expiration', 'headers', 'priority',
                   'reply_to', 'timestamp', 'type', 'user_id']
    _MESSAGE_ATTRS = ['delivery_tag', 'exchange', 'routing_key', 'redelivered']

    def __getattr__(self, item):
        """Get the attribute from the object

        :rtype: any
        :raises: AttributeError

        """
        # Is it one of the core consumer attributes?
        if item == 'channel':
            return self.channel
        elif item == 'config':
            return self.config
        elif item == 'message':
            return self.message

        # Return message properties
        if item in self._PROPERTIES:
            if item in self._INTEGER_PROPERTIES:
                return int(getattr(self.message.properties, item))
            if item == 'headers':
                return copy.deepcopy(self.message.properties.headers)
            return getattr(self.message.properties, item)

        # Is it a rejected.data.Message attribute
        if item in self._MESSAGE_ATTRS:
            return getattr(self.message.properties, item)

        # No dice?
        raise AttributeError

    @property
    def datetime(self):
        """Return the datetime of the message from the properties.timestamp

        :rtype: datetime.datetime

        """
        if not self.message.properties.timestamp:
            return None
        float_value = float(self.message.properties.timestamp)
        return datetime.datetime.fromtimestamp(float_value)
