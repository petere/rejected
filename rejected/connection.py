"""
Reusable Connection class for both consumers and the MCP for stats polling

"""
__since__ = '2012-12-02'

import logging
import pika
from pika import spec
from pika.adapters import tornado_connection
import urllib

from rejected import state

LOGGER = logging.getLogger(__name__)


class Connection(state.StatefulObject):
    """A common connection class that can be used by the MCP for queue depth
    checking and by workers for consuming and publishing messages.

    """
    QOS_PREFETCH = 1
    HEARTBEAT_INTERVAL = 30

    # State constants
    STATE_INITIALIZING = 0x01
    STATE_CONNECTING = 0x02
    STATE_OPENING_CHANNEL = 0x03
    STATE_CONNECTED = 0x04
    STATE_CLOSE_REQUESTED = 0x05
    STATE_CLOSING = 0x06
    STATE_CLOSED = 0x07

    # For reverse lookup
    _STATES = {0x01: 'Initializing',
               0x02: 'Connecting',
               0x03: 'Opening Channel',
               0x04: 'Connected',
               0x05: 'Close Requested',
               0x06: 'Closing',
               0x07: 'Closed'}

    def __init__(self, host, port, virtual_host, username, password, on_open,
                 heartbeat_interval=None, frame_max_size=None):
        super(Connection, self).__init__()

        self._host = host
        self._port = port
        self._virtual_host = virtual_host
        self._username = username
        self._password = password
        self._heartbeat = heartbeat_interval or self.HEARTBEAT_INTERVAL
        self._frame_max = frame_max_size or spec.FRAME_MAX_SIZE

        # Start with empty assigned attributes
        self._connection = None
        self._channel = None
        self._on_open = on_open
        self._on_close = None

    def __repr__(self):
        """Return a text description of the connection

        :rtype: str

        """
        return '<Connection "%s" connected=%s>' % (self, self.is_connected)

    def __str__(self):
        """Return a text description of the connection

        :rtype: str

        """
        return 'amqp://%s@%s:%i/%s' % (self._username,
                                       self._host, self._port,
                                       urllib.quote(self._virtual_host,
                                                    safe=""))

    def ack_message(self, delivery_tag):
        """Acknowledge the message on the broker

        :param str delivery_tag: Delivery tag to acknowledge

        """
        self._channel.basic_ack(delivery_tag=delivery_tag)

    def cancel_consumer(self, consumer_tag):
        """Tell RabbitMQ the process no longer wants to consumer messages.

        :param str consumer_tag: The consumer tag to cancel

        """
        if self._channel and self._channel.is_open:
            self._channel.basic_cancel(consumer_tag=consumer_tag)

    @property
    def channel(self):
        """Return the channel

        :rtype: pika.channel.Channel
        :raises: ChannelNotOpenException

        """
        if not self._channel or not self._channel.is_open:
            raise ChannelNotOpenException
        return self._channel

    def close(self):
        """This method closes the connection to RabbitMQ.

        :raises: ConnectionNotOpenException

        """
        if not self._channel or not self._channel.is_open:
            raise ConnectionNotOpenException
        LOGGER.info('Closing connection')
        self._set_state(self.STATE_CLOSING)
        self._connection.close()

    def consume(self, queue_name, callback, ack, consumer_tag):
        """Issue a Basic.Consume to RabbitMQ for the given queue_name, setting
        the consumer tag to the value of consumer_tag, the callback to the
        specified callback and no_ack to the inverse of ack.

        :param str queue_name: The name of the queue to consume from
        :param method callback: The callback method when a message is received
        :param bool ack: Should messages be explicitly acknowledged
        :param str consumer_tag: The consumer tag to use

        """
        self._channel.basic_consume(callback, queue_name, no_ack=not ack,
                                    consumer_tag=consumer_tag)

    @property
    def is_connected(self):
        """Returns a bool if the connection and channel are connected

        :rtype: bool

        """
        return self._state == self.STATE_CONNECTED

    def start(self):
        """Start the connection's IOLoop"""
        self._connection = self._connect()
        self._connection._ioloop.start()

    def reject_message(self, delivery_tag, requeue=True):
        """Reject the message on the broker and log it

        :param str delivery_tag: Delivery tag to reject
        :param bool requeue: Specify if the message should be requeued or not

        """
        self._channel.basic_reject(delivery_tag=delivery_tag, requeue=requeue)

    def set_prefetch_count(self, value):
        """Set the QoS prefetch value on the connection.

        :param int value: The value to set the prefetch count to

        """
        if not self.is_connected:
            raise ConnectionNotOpenException
        self._channel.basic_qos(None, 0, value, True)

    def _add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        LOGGER.info('Adding connection close callback')
        self._connection.add_on_close_callback(self._on_connection_closed)

    def _connect(self):
        """Connect to RabbitMQ returning the connection handle.

        :rtype: pika.adapters.tornado_connection.TornadoConnection

        """
        LOGGER.debug('Connecting to %s:%i:%s as %s',
                     self._host, self._port, self._virtual_host, self._username)
        self._set_state(self.STATE_CONNECTING)
        return tornado_connection.TornadoConnection(self._connection_parameters,
                                                    self._on_connection_open,
                                                    True)

    @property
    def _connection_parameters(self):
        """Return connection parameters for a pika connection.

        :rtype: pika.ConnectionParameters

        """
        return pika.ConnectionParameters(self._host,
                                         self._port,
                                         self._virtual_host,
                                         pika.PlainCredentials(self._username,
                                                               self._password),
                                         frame_max=self._frame_max,
                                         heartbeat_interval=self._heartbeat)

    def _on_channel_closed(self, method_frame):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as redeclare an exchange or queue with
        different paramters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.frame.Method method_frame: The Channel.Close method frame

        """
        LOGGER.warning('Channel was closed: (%s) %s',
                       method_frame.method.reply_code,
                       method_frame.method.reply_text)
        self._connection.close()

    def _on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self._add_on_channel_close_callback()
        self._on_open()
        self._set_state(self.STATE_CONNECTED)

    def _on_connection_closed(self, method_frame):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.frame.Method method_frame: The method frame from RabbitMQ

        """
        LOGGER.warning('Server closed connection, reopening: (%s) %s',
                       method_frame.method.reply_code,
                       method_frame.method.reply_text)
        self._set_state(self.STATE_CLOSED)
        self._channel = None

    def _on_connection_open(self, unused):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused: pika.adapters.tornado_connection.TornadoConnection

        """
        LOGGER.info('Connection opened')
        self._add_on_connection_close_callback()
        self._connection.channel(self._on_channel_open)


class ChannelNotOpenException(Exception):
    def __repr__(self):
        return "Channel is not open"


class ConnectionNotOpenException(Exception):
    def __repr__(self):
        return "Channel is not open"
