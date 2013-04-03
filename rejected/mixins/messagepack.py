"""MessagePack consumer mixins that automatically deserialize messages upon
 receipt and serialize messages when publishing. For more information on
 MessagePack see http://http://msgpack.org

"""
try:
    import msgpack
except ImportError:
    import msgpack_pure as msgpack

from rejected import consumer
from rejected.mixins import content


class MsgPackConsumer(content.DeserializingConsumer):
    """Automatically deserialize a message if it is received with a
    content-type property value of application/x-msgpack.

    """
    WARN_ON_UNSUPPORTED = False

    def _load_msgpack_value(self, value):
        """Deserialize a MessagePack value

        :param str value: The msgpack encoded string
        :rtype: any

        """
        return msgpack.load(value)

    def _receive(self, message):
        """Receive the message from RabbitMQ. To implement logic for processing
        a message, extend Consumer.process, not this method.

        This receive method inspects the content-type property and if set to
        application/x-msgpack, it will automatically deserialize the content,
        and it will set the content-type property to None.

        If the content-type property is not application/x-msgpack, it will
        pass the deserialization through to the
        rejected.mixins.DeseralizingConsumer.

        :param rejected.data.Message message: The message
        :raises: rejected.exceptions.MessageException

        """
        if message.properties.content_type == 'application/x-msgpack':
            message.body = self._load_msgpack_value(message.body)
            message.properties.content_type = None
        super(MsgPackConsumer, self)._receive(message)


class MsgPackPublishingConsumer(consumer.PublishingConsumer):
    """The MsgPackPublishingConsumer will automatically serialize the message
    body passed in as MsgPack, updating the content-type appropriately.

    """
    def publish(self, exchange, routing_key, body, properties=None):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on, automatically serializing the message body.

        If you would like to use this mixin with one of the encoding mixins
        such as the Bzip2PublishingConsumer, ZlibPublishingConsumer, or
        Base64PublishingConsumer, ensure that you specify this mixin before
        these mixins in your class declaration syntax.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param rejected.data.Properties: The message properties
        :param dict|list: The message body to publish

        """
        properties.content_type = 'application/x-msgpack'
        super(MsgPackPublishingConsumer, self).publish(exchange,
                                                       routing_key,
                                                       msgpack.dumps(body),
                                                       properties)
