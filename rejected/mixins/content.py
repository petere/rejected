"""Content encoding and serialization mixins for both automatically decoding
and deserializing messages when they are received and for automatically
encoding and serializing message bodies when publishing.

"""
import base64
import bz2
import csv
import logging
import pickle
import plistlib
try:
    import simplejson as json
except ImportError:
    import json
import StringIO as stringio
import yaml
import zlib

from rejected import consumer

LOGGER = logging.getLogger(__name__)


class DecodingConsumer(consumer.BaseConsumer):
    """Dynamically decode the incoming message body.

    Supported encodings:

        - base64
        - bzip2
        - zlib

    """
    def _decode_base64(self, value):
        """Return a base64 decoded value

        :param str value: Compressed value
        :rtype: str

        """
        return base64.b64decode(value)

    def _decompress_bz2(self, value):
        """Return a bz2 decompressed value

        :param str value: Compressed value
        :rtype: str

        """
        return bz2.decompress(value)

    def _decompress_zlib(self, value):
        """Return a zlib decompressed value

        :param str value: Compressed value
        :rtype: str

        """
        return zlib.decompress(value)

    def _receive(self, message):
        """Receive the message from RabbitMQ.

        This receive method decodes the message body based upon the content
        header and supports base64, bzip2, and zlib

        :param rejected.data.Message message: The message
        :raises: rejected.exceptions.MessageException

        """
        if message.properties.content_encoding == 'base64':
            message.body = self._decode_base64(message.body)
            message.properties.content_type = None
        elif message.properties.content_encoding == 'bzip2':
            message.body = self._decompress_bz2(message.body)
            message.properties.content_type = None
        elif message.properties.content_encoding == 'zlib':
            message.body = self._decompress_zlib(message.body)
            message.properties.content_type = None
        else:
            LOGGER.warning('Unsupported message encoding: %s',
                           message.properties.content_encoding)
        super(DecodingConsumer, self)._receive(message)


class DeserializingConsumer(consumer.BaseConsumer):
    """Dynamically deserialize the incoming message body.

    Supported content-types:

        - CSV: text/csv
        - JSON: application/json
        - Pickle: application/pickle, application/x-pickle,
            application/x-vnd.python.pickle, and application/vnd.python.pickle
        - Plist
        - YAML

    For MessagePack support check out the MsgPackConsumer mixin and for XML
    or HTML support check out the BS4Consumer class. Both mixins support
    additional content-types that this class does not support.

    """
    BS4_MIME_TYPES = ['text/html', 'text/xml']
    PICKLE_MIME_TYPES = ['application/pickle',
                         'application/x-pickle',
                         'application/x-vnd.python.pickle',
                         'application/vnd.python.pickle']
    YAML_MIME_TYPES = ['text/yaml', 'text/x-yaml']
    WARN_ON_UNSUPPORTED = True

    def _load_csv_value(self, value):
        """Create a csv.DictReader instance for the sniffed dialect for the
        value passed in.

        :param str value: The CSV value
        :rtype: csv.DictReader

        """
        csv_buffer = stringio.StringIO(value)
        dialect = csv.Sniffer().sniff(csv_buffer.read(1024))
        csv_buffer.seek(0)
        return csv.DictReader(csv_buffer, dialect=dialect)

    def _load_json_value(self, value):
        """Deserialize a JSON string returning the native Python data type
        for the value.

        :param str value: The JSON string
        :rtype: object

        """
        if json.__name__ == 'simplejson':
            return json.loads(value, use_decimal=True)
        return json.loads(value)

    def _load_pickle_value(self, value):
        """Deserialize a pickle string returning the native Python data type
        for the value.

        :param str value: The pickle string
        :rtype: object

        """
        return pickle.loads(value)

    def _load_plist_value(self, value):
        """Deserialize a plist string returning the native Python data type
        for the value.

        :param str value: The pickle string
        :rtype: dict

        """
        return plistlib.readPlistFromString(value)

    def _load_yaml_value(self, value):
        """Load an YAML string into an dict object.

        :param str value: The YAML string
        :rtype: any

        """
        return yaml.load(value)

    def _receive(self, message):
        """Receive the message from RabbitMQ. To implement logic for processing
        a message, extend Consumer.process, not this method.

        This receive method deserializes the incoming message body, replacing
        the message body with the deserialized value.

        :param rejected.data.Message message: The message
        :raises: rejected.exceptions.MessageException

        """
        if message.properties.content_type == 'application/json':
            message.body = self._load_json_value(message.body)
            message.properties.content_type = None
        elif message.properties.content_type in self.PICKLE_MIME_TYPES:
            message.body = self._load_pickle_value(message.body)
            message.properties.content_type = None
        elif message.properties.content_type == 'application/x-plist':
            message.body = self._load_plist_value(message.body)
            message.properties.content_type = None
        elif message.properties.content_type == 'text/csv':
            message.body = self._load_csv_value(message.body)
            message.properties.content_type = None
        elif yaml and message.properties.content_type in self.YAML_MIME_TYPES:
            message.body = self._load_yaml_value(message.body)
            message.properties.content_type = None
        elif self.WARN_ON_UNSUPPORTED:
            LOGGER.warning('Unsupported message type: %s',
                           message.properties.content_type)
        super(DeserializingConsumer, self)._receive(message)


class Base64PublishingConsumer(consumer.PublishingConsumer):
    """The Base64PublishingConsumer will automatically compress the message body
    when publishing and adds the appropriate content-encoding property.

    """
    def publish(self, exchange, routing_key, body, properties=None):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on.

        In this consumer, the body must be a str or unicode value. If you
        would like to pass in a dict or other serializable object, use
        one of the serialization mixins. In creating a class that uses
        both an encoding and serialization plugin, ensure the serialization
        plugin is listed first in the class construction.

        properties should be an instance of the rejected.data.Properties
        class or None.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param rejected.data.Properties: The message properties
        :param str|unicode body: The message body to publish

        """
        properties.content_encoding = 'base64'
        super(Base64PublishingConsumer, self).publish(exchange,
                                                      routing_key,
                                                      base64.b64encode(body),
                                                      properties)


class Bzip2PublishingConsumer(consumer.PublishingConsumer):
    """The Bzip2PublishingConsumer will automatically compress the message body
    when publishing and adds the appropriate content-encoding property.

    """
    def publish(self, exchange, routing_key, body, properties=None):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on.

        In this consumer, the body must be a str or unicode value. If you
        would like to pass in a dict or other serializable object, use
        one of the serialization mixins. In creating a class that uses
        both an encoding and serialization plugin, ensure the serialization
        plugin is listed first in the class construction.

        properties should be an instance of the rejected.data.Properties
        class or None.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param rejected.data.Properties: The message properties
        :param str|unicode body: The message body to publish

        """
        properties.content_encoding = 'bzip2'
        super(Bzip2PublishingConsumer, self).publish(exchange,
                                                     routing_key,
                                                     bz2.compress(body),
                                                     properties)


class ZlibPublishingConsumer(consumer.PublishingConsumer):
    """The ZlibPublishingConsumer will automatically compress the message body
    when publishing and adds the appropriate content-encoding property.

    """
    def publish(self, exchange, routing_key, body, properties=None):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on.

        In this consumer, the body must be a str or unicode value. If you
        would like to pass in a dict or other serializable object, use
        one of the serialization mixins. In creating a class that uses
        both an encoding and serialization plugin, ensure the serialization
        plugin is listed first in the class construction.

        properties should be an instance of the rejected.data.Properties
        class or None.

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to publish with
        :param rejected.data.Properties: The message properties
        :param str|unicode body: The message body to publish

        """
        properties.content_encoding = 'zlib'
        super(ZlibPublishingConsumer, self).publish(exchange,
                                                    routing_key,
                                                    zlib.compress(body),
                                                    properties)


class JSONPublishingConsumer(consumer.PublishingConsumer):
    """The JSONPublishingConsumer will automatically serialize the message body
    passed in as JSON, updating the content-type appropriately.

    """
    def _serialize(self, value):
        """Serialize the inbound value as JSON

        :param dict|list|str|number value: The value to serialize
        :return: str

        """
        return json.dumps(value, ensure_ascii=False)

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
        properties.content_type = 'application/json'
        super(JSONPublishingConsumer, self).publish(exchange,
                                                    routing_key,
                                                    self._serialize(body),
                                                    properties)


class PicklePublishingConsumer(consumer.PublishingConsumer):
    """The PicklePublishingConsumer will automatically serialize the message
    body passed in as pickled python, updating the content-type appropriately.

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
        :param object|dict|list: The message body to publish

        """
        properties.content_type = 'application/x-pickle'
        super(PicklePublishingConsumer, self).publish(exchange,
                                                      routing_key,
                                                      pickle.dumps(body),
                                                      properties)


class PlistPublishingConsumer(consumer.PublishingConsumer):
    """The PlistPublishingConsumer will automatically serialize the message
    body passed in as a plist, updating the content-type appropriately.

    """
    def _serialize(self, value):
        """Serialize the inbound value as a plist value.

        :param dict|list|str|number value: The value to serialize
        :return: str

        """
        return plistlib.writePlistToString(value)

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
        :param object|dict|list: The message body to publish

        """
        properties.content_type = 'application/x-plist'
        super(PlistPublishingConsumer, self).publish(exchange,
                                                     routing_key,
                                                     self._serialize(body),
                                                     properties)


class YAMLPublishingConsumer(consumer.PublishingConsumer):
    """The YAMLPublishingConsumer will automatically serialize the message body
    passed in as YAML, updating the content-type appropriately.

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
        properties.content_type = 'application/x-yaml'
        super(YAMLPublishingConsumer, self).publish(exchange,
                                                    routing_key,
                                                    yaml.dump(body),
                                                    properties)
