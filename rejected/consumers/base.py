"""
Base rejected message Consumer class

"""
import logging

LOGGER = logging.getLogger(__name__)


class Consumer(object):
    """Base Consumer class, when extending for consumer client usage, implement
    the process method and optionally the initialize method. It does not do much
    for consumer others.

    """
    def __init__(self, config):
        """Creates a new instance of a Consumer class. To perform
        initialization tasks, extend Consumer.initialize

        :param dict config: The configuration passed in from config file
                            "config" section for the consumer

        """
        self.config = configuration
        self.channel = None
        self.message = None
        self.initialize()

    def initialize(self):
        """Extend this method for any initialization tasks"""
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
        self._channel = channel
