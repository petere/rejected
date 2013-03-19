"""
Deserialize incoming messages for supported formats

"""
import csv
import logging
import pickle
import plistlib
import simplejson
import StringIO as stringio
import yaml

LOGGER = logging.getLogger(__name__)

from rejected.consumers import base
from rejected import exceptions

# Optional imports
try:
    import bs4
except ImportError:
    LOGGER.warning('BeautifulSoup not found, disabling html and xml support')
    bs4 = None

# Optional imports
try:
    import yaml
except ImportError:
    LOGGER.warning('BeautifulSoup not found, disabling html and xml support')
    yaml = None


class DeserializingConsumer(base.Consumer):
    """Dynamically deserialize the incoming message body.

    Supported content-types:

        - CSV
        - HTML
        - JSON
        - Pickle
        - Plist
        - XML
        - YAML

    """
    _BS4_MIME_TYPES = ['text/html', 'text/xml']
    _PICKLE_MIME_TYPES = ['application/pickle',
                          'application/x-pickle',
                          'application/x-vnd.python.pickle',
                          'application/vnd.python.pickle']
    _YAML_MIME_TYPES = ['text/yaml', 'text/x-yaml']


    def _load_bs4_value(self, value):
        """Load an HTML or XML string into an lxml etree object.

        :param str value: The HTML or XML string
        :rtype: bs4.BeautifulSoup
        :raises: ConsumerException

        """
        if not bs4:
            raise exceptions.ConsumerException('BeautifulSoup is not enabled')
        return bs4.BeautifulSoup(value)

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
        return simplejson.loads(value, use_decimal=True)

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
        elif message.properties.content_type in self._PICKLE_MIME_TYPES:
            message.body = self._load_pickle_value(message.body)
            message.properties.content_type = None
        elif message.properties.content_type == 'application/x-plist':
            message.body = self._load_plist_value(message.body)
            message.properties.content_type = None
        elif message.properties.content_type == 'text/csv':
            message.body = self._load_csv_value(message.body)
            message.properties.content_type = None
        elif bs4 and message.properties.content_type in self._BS4_MIME_TYPES:
            message.body = self._load_bs4_value(message.body)
            message.properties.content_type = None
        elif yaml and message.properties.content_type in self._YAML_MIME_TYPES:
            message.body = self._load_yaml_value(message.body)
            message.properties.content_type = None
        else:
            LOGGER.warning('Unsupported message type: %s',
                           message.properties.content_type)
        super(DeserializingConsumer, self)._receive(message)
