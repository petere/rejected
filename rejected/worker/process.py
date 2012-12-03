"""Consumer process management. Imports consumer code, manages RabbitMQ
connection state and collects stats about the consuming process.

"""
import clihelper
from pika import exceptions
import logging
from logging import config as logging_config
import multiprocessing
from os import path
import signal
import sys
import traceback

try:
    from newrelic import agent
    from newrelic.api import background_task as nr_btask
except ImportError:
    agent = None
    background_task = None

from rejected import __version__
from rejected import consumer
from rejected import connection
from rejected import data
from rejected import state

LOGGER = logging.getLogger(__name__)


def import_namespaced_class(namespaced_class):
    """Pass in a string in the format of foo.Bar, foo.bar.Baz, foo.bar.baz.Qux
    and it will return a handle to the class

    :param str namespaced_class: The namespaced class
    :return: tuple(Class, str)

    """
    LOGGER.debug('Importing %s', namespaced_class)
    # Split up our string containing the import and class
    parts = namespaced_class.split('.')

    # Build our strings for the import name and the class name
    import_name = '.'.join(parts[0:-1])
    class_name = parts[-1]

    import_handle = __import__(import_name, fromlist=class_name)
    if hasattr(import_handle, '__version__'):
        version = import_handle.__version__
    else:
        version = None

    # Return the class handle
    return getattr(import_handle, class_name), version


class WorkerProcess(multiprocessing.Process, state.StatefulObject):
    """Core process class that

    """
    ACK_DEFAULT = True

    _AMQP_APP_ID = 'rejected/%s' % __version__

    # Additional State constants
    STATE_PROCESSING = 0x04

    # Event constants
    ACK = 'ack'
    COMPLETE = 'complete'
    ERROR = 'error'
    EXCEPTION = 'exception'
    REDELIVERED = 'redelivered'
    REJECT = 'reject'
    START = 'start'

    # Locations to search for newrelic ini files
    INI_DIRS = ['.', '/etc/', '/etc/newrelic']
    INI_FILE = 'newrelic.ini'

    def __init__(self, group=None, target=None, name=None, args=(),
                 kwargs=None):
        super(WorkerProcess, self).__init__(group, target, name, args, kwargs)
        self._ack = kwargs['config'].get('ack', self.ACK_DEFAULT)
        self._config = kwargs['config']
        self._connection = None
        self._connection_config = kwargs['connection_config']
        self._consumer = None
        self._logging_config = kwargs['logging_config']
        self._queue_name = kwargs['config']['queue']
        self._state = self.STATE_INITIALIZING
        self._parent = self._kwargs['pipe']

        # Override ACTIVE with PROCESSING
        self._STATES[0x04] = 'Processing'

    def process(self, channel=None, method=None, header=None, body=None):
        """Process a message from Rabbit

        :param pika.channel.Channel channel: The channel the message was sent on
        :param pika.frames.MethodFrame method: The method frame
        :param pika.frames.HeaderFrame header: The header frame
        :param str body: The message body

        """
        if not self.is_idle:
            LOGGER.critical('Received a message while in state: %s',
                            self.state_description)
            return self._reject_message(method.delivery_tag)

        # Tell the parent a message was received
        self._send_event_to_parent(self.START)

        # Set our state to processing
        self._set_state(self.STATE_PROCESSING)
        LOGGER.debug('Received message #%s', method.delivery_tag)

        # Build the message wrapper object for all the parts
        message = data.Message(channel, method, header, body)
        if method.redelivered:
            self._send_event_to_parent(self.REDELIVERED)

        # Process the message, evaluating the success
        try:
            self._process(message)
        except KeyboardInterrupt:
            return self._stop()
        except exceptions.ChannelClosed as exception:
            self._record_exception(message.routing_key, exception, False)
            return self._stop()
        except exceptions.ConnectionClosed as exception:
            self._record_exception(message.routing_key, exception, False)
            return self._stop()
        except consumer.ConsumerException as exception:
            self._record_custom_metric('Requeued', message.routing_key)
            return self._on_error(method, exception)
        except consumer.MessageException as exception:
            self._record_custom_metric('Rejected', message.routing_key)
            return self._on_message_exception(method, exception)

        LOGGER.debug('Message processed')

        self._reset_state()

        # If no_ack was not set when we setup consuming, do so here
        if self._ack:
            self._ack_message(method.delivery_tag)
        else:
            self._send_event_to_parent(self.COMPLETE)

    def run(self):
        """Start the consumer"""
        self._setup_logging()
        self._initialize_newrelic()

        if not self._setup(self._config['queue'],
                           self._config['consumer'],
                           self._config.get('config', dict())):
            LOGGER.critical('Could not start consumer: %s',
                            self._config['consumer'])
            return

        # Block on the connection
        try:
            self._connection.start()
        except KeyboardInterrupt:
            self._stop()
            self._connection.start()

        LOGGER.debug('Exiting %s', self.name)

    def _ack_message(self, delivery_tag):
        self._connection.ack_message(delivery_tag)
        self._send_event_to_parent(self.ACK)

    @property
    def _connection_parameters(self):
        """Return the dictionary of configuration information for the connection

        :rtype: dict

        """
        config = self._connection_config
        parameters = dict()
        for key in ['host', 'port', 'virtual_host', 'username', 'password',
                    'heartbeat_interval', 'frame_max_size']:
            parameters[key] = config.get(key)
        parameters['on_open'] = self._on_connection_ready
        return parameters

    def _create_consumer(self, fqcn, config):
        """Import and create a new instance of the configured message consumer.

        :param str fqcn: The fully qualified class name
        :param dict config: The consumer specific configuration
        :rtype: instance
        :raises: ImportError

        """
        # Try and import the module
        try:
            consumer_class, version = import_namespaced_class(fqcn)
        except Exception as error:
            LOGGER.error('Error importing %s: %s', fqcn, error)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.extract_tb(exc_traceback)
            for line in lines:
                LOGGER.error(line)
            return False

        if version:
            LOGGER.info('Creating consumer %s v%s', fqcn, version)
        else:
            LOGGER.info('Creating consumer %s', fqcn)

        # If we have a config, pass it in to the constructor
        if 'config' in config:
            try:
                return consumer_class(config['config'])
            except Exception as error:
                LOGGER.error('Error creating %s: %s', fqcn, error)
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.extract_tb(exc_traceback)
                for line in lines:
                    LOGGER.error(line)
                return False

        # No config to pass
        try:
            return consumer_class({})
        except Exception as error:
            LOGGER.error('Error creating %s: %s', fqcn, error)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.extract_tb(exc_traceback)
            for line in lines:
                LOGGER.error(line)
            return False

    def _initialize_newrelic(self):
        """Initialize newrelic iterating through the paths looking for an ini
        file.

        :raises: EnvironmentError

        """
        if not agent:
            return

        if 'newrelic' in self._kwargs['config']:
            if not self._kwargs['config']['newrelic']:
                return
            LOGGER.debug('New Relic: %r', self._kwargs['config']['newrelic'])

        for ini_dir in self.INI_DIRS:
            ini_path = path.join(path.normpath(ini_dir), self.INI_FILE)
            if path.exists(ini_path) and path.isfile(ini_path):
                LOGGER.debug('Initializing NewRelic with %s', ini_path)
                self._instrument_consumer_with_newrelic()
                agent.initialize(ini_path)
                return

        # Since an ini was not found, disable a found agent
        #global agent
        #agent = None

    def _instrument_consumer_with_newrelic(self):
        """Wrap the consumer process method with the BackgroundTaskWrapper."""
        task = nr_btask.BackgroundTaskWrapper(consumer.Consumer.process,
                                              name=consumer.Consumer.name,
                                              group='Rejected')
        consumer.Consumer.process = task

    @property
    def _is_processing(self):
        """Returns a bool specifying if the consumer is currently processing

        :rtype: bool

        """
        return self._state in [self.STATE_PROCESSING, self.STATE_STOP_REQUESTED]

    def _new_connection(self):
        """Return a new Connection instance

        :rtype: rejected.connection.Connection

        """
        return connection.Connection(**self._connection_parameters)

    def _on_connection_ready(self):

        # Consume from the queue
        self._connection.consume(self._queue_name,
                                 self.process, self._ack, self.name)

        # Set our runtime state to idle now that it is connect
        self._set_state(self.STATE_IDLE)

    def _on_error(self, method, exception):
        """Called when a runtime error encountered.

        :param pika.frames.MethodFrame method: The method frame with an error
        :param consumer.ConsumerException exception: The error that occurred

        """
        self._reset_state()
        self._send_event_to_parent(self.ERROR)
        if self._ack:
            self._reject_message(method.delivery_tag)
        else:
            self._send_event_to_parent(self.COMPLETE)

    def _on_message_exception(self, method, exception):
        """Called when a consumer.MessageException is raised, will reject the
        message, not requeueing it.

        :param pika.frames.MethodFrame method: The method frame with an error
        :param consumer.MessageException exception: The error that occurred
        :raises: RuntimeError

        """
        LOGGER.error('Message was rejected: %s', exception)
        self._reset_state()

        # Raise an exception if no_ack = True since the msg can't be rejected
        if not self._ack:
            LOGGER.critical('Can not rejected messages when ack is False')
            return self._stop()

        # Reject the message and do not requeue it
        self._reject_message(method.delivery_tag, False)

    def _on_ready_to_stop(self):

        # Set the state to shutting down if it wasn't set as that during loop
        self._set_state(self.STATE_SHUTTING_DOWN)

        # If the connection is still around, close it
        if self._connection.is_connected:
            LOGGER.debug('Closing connection to RabbitMQ')
            self._connection.close()

        # Allow the consumer to gracefully stop and then stop the IOLoop
        self._stop_consumer()

        # Note that shutdown is complete and set the state accordingly
        LOGGER.info('Shutdown complete')
        self._set_state(self.STATE_STOPPED)

    def _process(self, message):
        """Wrap the actual processor processing bits

        :param Message message: Message to process
        :raises: consumer.ConsumerException

        """
        # Try and process the message
        try:
            self._consumer.process(message)
        except consumer.ConsumerException as exception:
            self._record_exception(message.routing_key, exception, True)
            raise exception
        except consumer.MessageException as exception:
            self._record_custom_metric('BadMessage', message.routing_key)
            raise exception
        except Exception as exception:
            self._record_exception(message.routing_key, exception)
            raise consumer.ConsumerException(exception.__class__.__name__)

    def _record_custom_metric(self, metric_name, routing_key):
        if agent:
            agent.record_custom_metric('Rejected/%s/%s' % (metric_name,
                                                           routing_key), 1)

    def _record_exception(self, routing_key, exception, handled=False):
        if agent:
            exc_type, value, tb = sys.exc_info()
            agent.record_exception(exception, value, tb)
            if handled:
                metric = 'Handled Exception %s' % type(exception)
            else:
                metric = str(exception.__class__.__name__)
            self._record_custom_metric(metric, routing_key)

        if not handled:
            formatted_lines = traceback.format_exc().splitlines()
            LOGGER.critical('Processor threw an uncaught exception %s: %s',
                            type(exception), exception)
            for offset, line in enumerate(formatted_lines):
                LOGGER.info('(%s) %i: %s', type(exception), offset,
                            line.strip())
            self._send_event_to_parent(self.EXCEPTION)

    def _reject_message(self, delivery_tag, requeue=True):
        """Reject the message on the broker and log it

        :param str delivery_tag: Delivery tag to reject
        :param bool requeue: Specify if the message should be requeued or not

        """
        self._connection.reject_message(delivery_tag, requeue)
        self._send_event_to_parent(self.REJECT)

    def _reset_state(self):
        """Reset the runtime state after processing a message to either idle
        or shutting down based upon the current state.

        """
        if agent:
            agent.end_of_transaction()

        if self.is_waiting_to_shutdown:
            self._set_state(self.STATE_SHUTTING_DOWN)
            self._on_ready_to_stop()
        elif self._is_processing:
            self._set_state(self.STATE_IDLE)
        else:
            LOGGER.critical('Unexepected state: %s', self.state_description)

    def _setup(self, queue_name, fqcn, consumer_config):
        """Initialize the consumer, setting up needed attributes and connecting
        to RabbitMQ.

        :param str queue_name: The queue name to consume from
        :param str fqcn: Fully qualified consumer name
        :param dict consumer_config: consumer specific configuration
        :rtype: bool

        """
        self._consumer = self._create_consumer(fqcn, consumer_config)
        if not self._consumer:
            LOGGER.critical('Could not import and start processor')
            return False

        # Setup the signal handler for stats
        self._setup_signal_handlers()

        # Create the RabbitMQ Connection
        self._connection = self._new_connection()


        return True

    def _send_event_to_parent(self, event, value=None):
        self._parent.send({'event': event, 'value': value})

    def _setup_logging(self):
        for handler in  self._logging_config['handlers']:
            if 'debug_only' in self._logging_config['handlers'][handler]:
                del  self._logging_config['handlers'][handler]['debug_only']
        logging_config.dictConfig(self._logging_config)

    def _setup_signal_handlers(self):
        """Setup the stats and stop signal handlers. Use SIGABRT instead of
        SIGTERM due to the multiprocessing's behavior with SIGTERM.

        """
        signal.signal(signal.SIGABRT, self._stop)
        signal.siginterrupt(signal.SIGABRT, False)

    def _stop(self, signum=None, frame_unused=None):
        """Stop the consumer from consuming by calling BasicCancel and setting
        our state.

        """
        LOGGER.debug('Stop called in state: %s', self.state_description)
        if self.is_stopped:
            LOGGER.debug('Stop requested but consumer is already stopped')
            return
        elif self.is_shutting_down:
            LOGGER.debug('Stop requested but consumer is already shutting down')
            return
        elif self.is_waiting_to_shutdown:
            LOGGER.debug('Stop requested but already waiting to shut down')
            return

        LOGGER.info('Shutting down')

        # Stop consuming
        self._connection.cancel_consumer(self.name)

        # Wait until the consumer has finished processing to shutdown
        if self._is_processing:
            self._set_state(self.STATE_STOP_REQUESTED)
            if signum == signal.SIGABRT:
                signal.siginterrupt(signal.SIGABRT, False)
            return

        self._on_ready_to_stop()

    def _stop_consumer(self):
        """Stop the consumer object and allow it to do a clean shutdown if it
        has the ability to do so.

        """
        try:
            LOGGER.info('Shutting down the consumer')
            self._consumer.shutdown()
        except AttributeError:
            LOGGER.debug('Consumer does not have a shutdown method')
