__author__ = 'gmr'

"""
The Attribute Consumer assigns all aspects of the message to attributes of the
consumer class.

"""
import copy
import datetime
import logging

LOGGER = logging.getLogger(__name__)

from rejected.consumers import base


class PublishingConsumer(base.Consumer):
    """The consumer will allow for pubishing of message
    """
    def _publish_message(self, exchange, routing_key, properties, body,
                         no_serialization=False, no_encoding=False):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on.

        By default, if you pass a non-string object to the body and the
        properties have a supported content-type set, the body will be
        auto-serialized in the specified content-type.

        If the properties do not have a timestamp set, it will be set to the
        current time.

        If you specify a content-encoding in the properties and the encoding is
        supported, the body will be auto-encoded.

        Both of these behaviors can be disabled by setting no_serialization or
        no_encoding to True.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param rejected.data.Properties: The message properties
        :param no_serialization: Turn off auto-serialization of the body
        :param no_encoding: Turn off auto-encoding of the body

        """
        # Convert the rejected.data.Properties object to a pika.BasicProperties
        LOGGER.debug('Converting properties')
        properties_out = self._get_pika_properties(properties)

        # Auto-serialize the content if needed
        if (not no_serialization and not isinstance(body, basestring) and
            properties.content_type):
            LOGGER.debug('Auto-serializing message body')
            body = self._auto_serialize(properties.content_type, body)

        # Auto-encode the message body if needed
        if not no_encoding and properties.content_encoding:
            LOGGER.debug('Auto-encoding message body')
            body = self._auto_encode(properties.content_encoding, body)

        # Publish the message
        LOGGER.debug('Publishing message to %s:%s', exchange, routing_key)
        self._channel.basic_publish(exchange=exchange,
                                    routing_key=routing_key,
                                    properties=properties_out,
                                    body=body)
