"""Validating consumers can ensure that either the expiration has not expired
or that the message type is supported by the consumer.

"""
import logging
import time

from rejected import exceptions
from rejected import consumer

LOGGER = logging.getLogger(__name__)


class ExpirationValidation(consumer.BaseConsumer):
    """The ExpirationValidation checks the expiration property, casting it
    to an integer and drops the message has expired.

    """
    @property
    def message_has_expired(self):
        """Return a boolean evaluation of if the message has expired. If
        expiration is not set, always return False

        :rtype: bool

        """
        if not self.message.properties.expiration:
            return False
        return time.time() >= int(self.message.properties.expiration)

    def _receive(self, message):
        """Receive the message from RabbitMQ. To implement logic for processing
        a message, extend Consumer.process, not this method.

        This receive method validates the message expiration

        :param rejected.Consumer.Message message: The message to process
        :raises: pika.exceptions.MessageException

        """
        self.message = message
        if self.message_has_expired:
            LOGGER.debug('Message expired %i seconds ago, dropping.',
                         time.time() - self.message.properties.expiration)
            raise exceptions.MessageException('Message expired')
        super(ExpirationValidation, self)._receive(message)


class TypeValidation(consumer.BaseConsumer):
    """TypeValidation validates the message type received is in the
    list of MESSAGE_TYPES specified by a child class.

    If DROP_EXPIRED_MESSAGES is True and a message has the expiration property
    set and the expiration has occurred, the message will be dropped.

    """
    MESSAGE_TYPES = []

    def _receive(self, message):
        """Receive the message from RabbitMQ. To implement logic for processing
        a message, extend Consumer.process, not this method.

        This receive method validates the message type property is supported

        :param rejected.Consumer.Message message: The message to process
        :raises: pika.exceptions.MessageException

        """
        self.message = message
        if self.message.properties.type not in self.MESSAGE_TYPES:
            LOGGER.warning('Received a non-supported message type: %s',
                           self.message.properties.type)
            raise exceptions.MessageException('Invalid message type: %s',
                                              self.message.properties.type)
        super(TypeValidation, self)._receive(message)
