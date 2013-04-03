"""Base rejected message Consumer classes"""
import logging
import pika
import time
import uuid

from rejected import data

LOGGER = logging.getLogger(__name__)


class BaseConsumer(object):
    """Base Consumer class, when extending for consumer client usage, implement
    the process method and optionally the initialize method. It does not do much
    for consumer others.

    """
    def __init__(self, configuration):
        """Creates a new instance of a Consumer class. To perform
        initialization tasks, extend Consumer.initialize

        :param dict configuration: The configuration passed in from config
                file "config" section for the consumer

        """
        self.config = configuration
        self.channel = None
        self.message = None
        self.initialize()

    def initialize(self):
        """Extend this method for any initialization tasks, when using a Mix-in
        class, make sure to call super(YourClass, self).initialize() if
        implementing the method.

        """
        pass

    def process(self):
        """Extend this method for implementing your Consumer logic.

        If the message can not be processed and the Consumer should stop after
        n failures to process messages, raise the exceptions.ConsumerException.
        If the message is bad and should be rejected, raise
        exceptions.MessageException.

        exceptions.ConsumerException will requeue the message for reprocessing
        exceptions.MessageException will nack the message. If you have a
        dead-letter exchange, the message will be sent there.

        :raises: pika.exceptions.ConsumerException
        :raises: NotImplementedError

        """
        raise NotImplementedError

    def name(self, *args, **kwargs):
        """Return the consumer class name.

        :rtype: str

        """
        return self.__class__.__name__

    def _receive(self, message):
        """Receive the message from RabbitMQ. To implement logic for processing
        a message, extend Consumer.process, not use this method.

        :param rejected.data.Message message: The message

        """
        self.message = message
        self.process()

    def _set_channel(self, channel):
        """Assign the _channel attribute to the channel that was passed in.

        :param pika.channel.Channel channel: The channel to assign

        """
        self.channel = channel


class PublishingConsumer(BaseConsumer):
    """The consumer expands on the BaseConsumer to allow for publishing on
    the same channel that a message is consumed from.

    """
    def publish(self, exchange, routing_key, body, properties=None):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on.

        In this consumer, the body must be a str or unicode value. If you
        would like to pass in a dict or other serializable object, use
        one of the serialization mixins from the content mixin module.

        If you would like to compress the body, use one of the compression
        mixins in the content mixin module.

        properties should be an instance of the rejected.data.Properties
        class or None.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param rejected.data.Properties: The message properties
        :param str|unicode body: The message body to publish

        """
        if properties:
            properties = self._get_pika_properties(properties)
        self.channel.basic_publish(exchange=exchange,
                                   routing_key=routing_key,
                                   properties=properties,
                                   body=body)

    def _get_pika_properties(self, properties_in):
        """Return a pika.BasicProperties object for a rejected.data.Properties
        object.

        :param rejected.data.Properties properties_in: Properties to convert
        :rtype: pika.BasicProperties

        """
        properties = pika.BasicProperties()
        if properties_in.app_id is not None:
            properties.app_id = properties_in.app_id
        if properties_in.content_encoding is not None:
            properties.content_encoding = properties_in.content_encoding
        if properties_in.content_type is not None:
            properties.content_type = properties_in.content_type
        if properties_in.correlation_id is not None:
            properties.correlation_id = properties_in.correlation_id
        if properties_in.delivery_mode is not None:
            properties.delivery_mode = properties_in.delivery_mode
        if properties_in.expiration is not None:
            properties.expiration = str(properties_in.expiration)
        if properties_in.headers is not None and \
                len(properties_in.headers.keys()):
            properties.headers = dict(properties_in.headers)
        if properties_in.priority is not None:
            properties.priority = properties_in.priority
        if properties_in.reply_to is not None:
            properties.reply_to = properties_in.reply_to
        if properties_in.message_id is not None:
            properties.message_id = properties_in.message_id
        properties.timestamp = properties_in.timestamp or int(time.time())
        if properties_in.type is not None:
            properties.type = properties_in.type
        if properties_in.user_id is not None:
            properties.user_id = properties_in.user_id
        return properties


class RPCConsumer(PublishingConsumer):
    """The RPCConsumer implements methods for replying to messages received
    using the reply_to property or a value passed in as the routing key.

    Use the the various serialization and encoding mixins in
    rejected.mixins.content if you would like the body to automatically be
    serialized and/or compressed prior to sending.

    """
    def reply(self,
              body,
              properties=None,
              exchange=None,
              routing_key=None,
              auto_id=True):
        """Reply to the received message.

        By default, the reply would be sent to the same exchange the
        original message was sent on. To change this behavior, pass an
        exchange to use into the method. Any value passed in will overwrite
        the exchange the original message was published to.

        By default, the message is routed to the routing key in the reply_to
        property of the original message. If a different routing key is
        desired, pass it in as the routing key parameter. Like the exchange
        parameter, if it is passed, it will overwrite any value in the
        original message properties. A routing key must be set or the
        reply_to must be set in the original message. If it is not, an
        exception will be thrown.

        If reply_to is set in the original properties,
        it will be used as the routing key. If the reply_to is not set
        in the properties and it is not passed in, a ValueException will be
        raised. If reply to is set in the properties, it will be cleared out
        prior to the message being republished.

        If auto_id is True, a new uuid4 value will be generated for the
        message_id. The correlation_id will be set to the message_id of the
        original message. In addition, the timestamp will be assigned the
        current time of the message. If auto_id is False, neither the message_id
        or the correlation_id will be changed in the properties.

        Like the Publishing consumer, the body must be a str or unicode value.
        If you would like to pass in a dict or other serializable object, use
        one of the serialization mixins from the content mixin module.

        Also like the PublishingConsumer, if you would like to compress
        the body, use one of the compression mixins in the content
        mixin module.

        :param str|unicode body: The message body to publish
        :param rejected.data.Properties: The message properties
        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param bool auto_id: Use "automatic id" functionality described above
        :raises: ValueError

        """
        if not properties:
            properties = data.Properties()

        # Automatically assign the app_id and the timestamp
        properties.app_id = self.name()
        properties.timestamp = int(time.time())

        if auto_id and self.message.properties.message_id:
            properties.correlation_id = self.message.properties.message_id
            properties.message_id = str(uuid.uuid4())

        # Wipe out reply_to if it's set
        if properties.reply_to:
            properties.reply_to = None

        self.publish(exchange or self.message.exchange,
                     routing_key or self.message.properties.reply_to,
                     body,
                     properties)
