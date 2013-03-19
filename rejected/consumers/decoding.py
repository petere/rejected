"""
Automatically decode the incoming message by the content_encoding header value

"""
import base64
import bz2
import logging
import zlib

LOGGER = logging.getLogger(__name__)

from rejected.consumers import base


class DecodingConsumer(base.Consumer):
    """Dynamically decode the incoming message body.

    Supported encodings:

        - base64
        - bzip2
        - gzip

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

    def _decompress_gzip(self, value):
        """Return a zlib decompressed value

        :param str value: Compressed value
        :rtype: str

        """
        return zlib.decompress(value)

    def _receive(self, message):
        """Receive the message from RabbitMQ. To implement logic for processing
        a message, extend Consumer.process, not this method.

        This receive method decodes the message body based upon the content
        header and supports base64, bzip2, and gzip

        :param rejected.data.Message message: The message
        :raises: rejected.exceptions.MessageException

        """
        if message.properties.content_encoding == 'base64':
            message.body = self._decode_base64(message.body)
            message.properties.content_type = None
        elif message.properties.content_encoding == 'bzip2':
            message.body = self._decompress_bz2(message.body)
            message.properties.content_type = None
        elif message.properties.content_encoding == 'gzip':
            message.body = self._decompress_gzip(message.body)
            message.properties.content_type = None
        else:
            LOGGER.warning('Unsupported message encoding: %s',
                           message.properties.content_encoding)
        super(DecodingConsumer, self)._receive(message)
