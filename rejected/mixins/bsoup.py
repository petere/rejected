"""The Bs4Consumer class adds support for HTML and XML content"""
import bs4

from rejected.mixins import content


class Bs4Consumer(content.DeserializingConsumer):
    """The Bs4Consumer class adds HTML and XML support to the
    DeseralizingConsumer using the BeautifulSoup library. When a message is
    received, it inspects the content-type property and if the content-type
    contains a supported mime type (text/html or text/xml) it will
    automatically deserialize the message body.

    """
    BS4_MIME_TYPES = ['text/html', 'text/xml']
    WARN_ON_UNSUPPORTED = False

    def _load_bs4_value(self, value):
        """Load an HTML or XML string into an lxml etree object.

        :param str value: The HTML or XML string
        :rtype: bs4.BeautifulSoup
        :raises: ConsumerException

        """
        return bs4.BeautifulSoup(value)

    def _receive(self, message):
        """Receive the message from RabbitMQ. To implement logic for
        processing a message, extend Consumer.process, not this method.

        This receive method inspects the content-type property and if set to
        text/html or text/xml, it will automatically deserialize the content.
        Unlike the MsgPackConsumer, it will not reset the content-type
        property so that your consumer code can determine how it should treat
        the bs4 deserialized content.

        If the content-type property does not indicate a supported value,
        the mixin will pass the deserialization through to the
        rejected.mixins.DeseralizingConsumer.

        :param rejected.data.Message message: The message
        :raises: rejected.exceptions.MessageException

        """
        if message.properties.content_type in self.BS4_MIME_TYPES:
            message.body = self._load_bs4_value(message.body)
        super(Bs4Consumer, self)._receive(message)

