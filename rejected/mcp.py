"""
Master Control Program

"""
import clihelper
import logging
import multiprocessing
import os
import signal
import sys

from rejected import worker
from rejected import __version__

LOGGER = logging.getLogger(__name__)


class MasterControlProgram(clihelper.Controller):
    """The Master Control Program manages worker processes, checking state
    information, checking performance, worker counts and queue depth.

    MCP
      \-- Connection
      \-- HTTP Server
      \-- Worker
            \-- Connection
            \-- Consumer
      \-- Worker
            \-- Connection
            \-- Consumer
      \-- Worker
            \-- Connection
            \-- Consumer

    """
    WORKER = 'worker'
    WORKERS = 'workers'
    WORKERS_CREATED = 'workers_created'
    MIN = 'min'
    MAX = 'max'
    MAX_WORKERS = 'max_workers'
    MIN_WORKERS = 'min_workers'
    THREAD = 'thread'

    DEFAULT_POLL_INTERVAL = 30
    DEFAULT_WORKER_MIN_COUNT = 1
    DEFAULT_WORKER_MAX_COUNT = 2

    def __init__(self, options, arguments):
        """Initialize the Master Control Program

        :param optparse.Values options: OptionParser option values
        :param list arguments: Left over positional cli arguments

        """
        self._set_process_name()
        LOGGER.info('rejected v%s initializing', __version__)
        super(MasterControlProgram, self).__init__(options, arguments)
        self._worker_type = getattr(options, 'worker_type')
        self._workers = dict()

    def _cleanup(self):
        self._set_state(self._STATE_SHUTTING_DOWN)
        self._stop_all_workers()
        self._shutdown_complete()
        LOGGER.debug('Done')
        os.kill(os.getpid(), signal.SIGALRM)

    def _stop_workers(self, worker_type):
        for worker in self._workers[worker_type][self.WORKERS]:
            worker.stop()

    def _stop_all_workers(self):
        for worker_type in self._workers.keys():
            self._stop_workers(worker_type)

    def _connection_config(self, connection_name):
        """Return the connection configuration for the specified connection name

        :rtype: dict

        """
        return self._connection_configurations.get(connection_name, dict())

    @property
    def _connection_configurations(self):
        """Return the Connections configuration.

        :rtype: dict

        """
        return self._get_application_config().get('Connections', dict())

    def _create_worker(self, worker_type, name, connection):
        """Create the worker process object for the given worker type, name and
        connection

        :param str worker_type: The worker_type
        :param str name: The name of the child worker process
        :param str connection: The name of the connection

        """
        kwargs = {'worker_type': worker_type,
                  'worker_config': self._worker_config(worker_type),
                  'logging_config': self._config.get('Logging'),
                  'connection_config': self._connection_config(connection)}

        thread = worker.WorkerThread(name=name, kwargs=kwargs)
        self._workers[worker_type][self.WORKERS].append(thread)
        self._workers[worker_type][self.WORKERS_CREATED] += 1
        thread.start()

    def _create_workers(self, worker_type):
        """Create the child processes for the given worker type

        :param str worker_type: The worker type to create a child for

        """
        self._setup_worker_type(worker_type)
        worker_count = self._workers[worker_type][self.MIN_WORKERS]
        LOGGER.info('Creating %i %s worker processes per connection',
                    worker_count, worker_type)
        for connection in self._worker_connections(worker_type):
            for worker_number in xrange(0, worker_count):
                name = self._worker_name(worker_type)
                self._create_worker(worker_type, name, connection)

    def _prepend_python_path(self, path):
        """Add the specified value to the python path.

        :param str path: The path to append

        """
        LOGGER.debug('Prepending "%s" to the python path.', path)
        sys.path.insert(0, path)

    def _process(self):
        """On every polling period check all of the workers and reap dead
        processes and spawn new ones as needed.

        """
        LOGGER.debug('Woke from nap')

    def _set_process_name(self):
        """Set the process name for the top level process so that it shows up
        in logs in a more trackable fasion.

        """
        process = multiprocessing.current_process()
        for offset in xrange(0, len(sys.argv)):
            if sys.argv[offset] == '-c':
                name = sys.argv[offset + 1].split('/')[-1]
                process.name = 'rejected:%s' % name.split('.')[0]
                break

    def _setup(self):
        """When the consumer is ready to start running, kick off all of our
        consumer consumers and then loop while we process messages.

        """
        # Set the state to active
        self._set_state(self._STATE_RUNNING)

        # Get the consumer section from the config
        if 'Workers' not in self._get_application_config():
            LOGGER.error('Missing Consumers section of configuration, '
                         'aborting: %r', self._config)
            return self._set_state(self._STATE_SHUTTING_DOWN)

        # Strip consumers if a consumer is specified
        if self._worker_type:
            LOGGER.info('Limiting to %s workers', self._worker_type)
            for worker_type in self._worker_configuration.keys():
                if worker_type != self._worker_type:
                    LOGGER.debug('Removing %s for %s only processing',
                                 worker_type, self._worker_type)
                    del self._config['Workers'][worker_type]

        # Start the workers
        for worker_type in self._worker_configuration.keys():
            self._create_workers(worker_type)

    def _setup_worker_type(self, worker_type):
        """Add the worker_type to the workers stack with the default values"""
        if worker_type not in self._workers.keys():
            LOGGER.debug('Initializing %s', worker_type)
            config = self._worker_config(worker_type)
            min_workers = config.get(self.MIN, self.DEFAULT_WORKER_MIN_COUNT)
            max_workers = config.get(self.MAX, self.DEFAULT_WORKER_MAX_COUNT)
            self._workers[worker_type] = {self.WORKERS: list(),
                                          self.WORKERS_CREATED: 0,
                                          self.MIN_WORKERS: min_workers,
                                          self.MAX_WORKERS: max_workers}
            LOGGER.debug(self._workers[worker_type])

    def _worker_config(self, worker_type):
        """Return the worker configuration for the specified worker_type

        :rtype: dict

        """
        return self._worker_configuration.get(worker_type, dict())

    @property
    def _worker_configuration(self):
        """Return the Workers configuration.

        :rtype: dict

        """
        return self._get_application_config().get('Workers', dict())

    def _worker_connections(self, worker_type):
        """Return the connection names for the connections for the specified
        worker type.

        :param str worker_type: The worker type
        :rtype: list

        """
        worker = self._worker_configuration.get(worker_type) or dict()
        return worker.get('connections', list())

    def _worker_name(self, worker_type):
        """Return a unique name for the worker by appending the worker_type with
        which child number for this worker type it is.

        """
        child_num = self._workers[worker_type][self.WORKERS_CREATED] + 1
        return '%s-%i' % (worker_type, child_num)



def _cli_options(parser):
    """Add options to the parser

    :param optparse.OptionParser parser: The option parser to add options to

    """
    parser.add_option('-o', '--only',
                      action='store',
                      default=None,
                      dest='worker_type',
                      help='Only run the worker type specified')
    parser.add_option('-p', '--prepend-path',
                      action='store',
                      default=None,
                      dest='prepend_path',
                      help='Prepend the python path with the value.')


def start():
    """Called when invoking the command line script."""
    clihelper.setup('rejected', 'RabbitMQ consumer framework', __version__)
    try:
        clihelper.run(MasterControlProgram, _cli_options)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    start()
