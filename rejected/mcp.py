"""
Master Control Program

"""
import logging
import multiprocessing
import os
import Queue
import signal
import sys
import threading
import time

from rejected import process
from rejected import state
from rejected import __version__

LOGGER = logging.getLogger(__name__)


class MasterControlProgram(state.State):
    """Master Control Program keeps track of and manages consumer processes."""
    MIN_CONSUMERS = 1
    MAX_CONSUMERS = 2
    MAX_SHUTDOWN_WAIT = 10
    POLL_INTERVAL = 30.0
    POLL_RESULTS_INTERVAL = 3.0
    SHUTDOWN_WAIT = 1

    def __init__(self, config, logging_config, consumer=None, profile=None):
        """Initialize the Master Control Program

        :param dict config: The full content from the YAML config file
        :param str consumer: If specified, only run processes for this consumer
        :param str profile: Optional profile output directory to
                            enable profiling

        """
        self.set_process_name()
        LOGGER.info('rejected v%s initializing', __version__)
        super(MasterControlProgram, self).__init__()

        # Default values
        self.consumer = consumer
        self.consumers = dict()
        self.config = config
        self.last_poll_results = dict()
        self.logging_config = logging_config
        self.poll_data = {'time': 0, 'processes': list()}
        self.poll_timer = None
        self.profile = profile
        self.results_timer = None
        self.stats = dict()
        self.stats_queue = multiprocessing.Queue()

        # Carry for logging internal stats collection data
        self.log_stats_enabled = config.get('log_stats', False)
        LOGGER.debug('Stats logging enabled: %s', self.log_stats_enabled)

        # Setup the poller related threads
        self.poll_interval = config.get('poll_interval', self.POLL_INTERVAL)
        LOGGER.debug('Set process poll interval to %.2f', self.poll_interval)

    @property
    def active_processes(self):
        """Return a list of all active processes, pruning dead ones

        :rtype: list

        """
        dead_processes = list()
        active_processes = multiprocessing.active_children()
        for process in active_processes:
            if not process.is_alive():
                dead_processes.append(process)
        if dead_processes:
            LOGGER.debug('Found %i dead processes to remove',
                         len(dead_processes))
            for process in dead_processes:
                self.remove_process(process.name)
                active_processes.remove(process)
        return active_processes

    def calculate_stats(self, data):
        """Calculate the stats data for our process level data.

        :param data: The collected stats data to report on
        :type data: dict

        """
        timestamp = data['timestamp']
        del data['timestamp']
        LOGGER.debug('Calculating stats for data timestamp: %i', timestamp)

        # Iterate through the last poll results
        stats = self.consumer_stats_counter()
        consumer_stats = dict()
        for name in data.keys():
            consumer_stats[name] = self.consumer_stats_counter()
            consumer_stats[name]['processes'] = \
                self.process_count_by_consumer(name)
            for process in data[name].keys():
                for key in stats:
                    value = data[name][process]['counts'][key]
                    stats[key] += value
                    consumer_stats[name][key] += value

        # Return a data structure that can be used in reporting out the stats
        stats['processes'] = len(self.active_processes)
        return {'last_poll': timestamp,
                'consumers': consumer_stats,
                'process_data': data,
                'counts': stats}

    def calculate_velocity(self, counts):
        """Calculate the message velocity to determine how many messages are
        processed per second.

        :param dict counts: The count dictionary to use for calculation
        :rtype: float

        """
        total_time = counts['idle_time'] + counts['processing_time']
        if total_time == 0 or counts['processed'] == 0:
            LOGGER.debug('Returning 0')
        return float(counts['processed']) / float(total_time)

    def check_consumer_process_counts(self):
        """Check for the minimum consumer process levels and start up new
        processes needed.

        """
        LOGGER.debug('Checking minimum consumer process levels')
        for name in self.consumers:
            for connection in self.consumers[name]['connections']:
                processes_needed = self.process_spawn_qty(name, connection)
                if processes_needed:
                    LOGGER.debug('Need to spawn %i processes for %s on %s',
                                 processes_needed, name, connection)
                    self.start_processes(name, connection, processes_needed)

    def consumer_dict(self, configuration):
        """Return a consumer dict for the given name and configuration.

        :param dict configuration: The consumer configuration
        :rtype: dict

        """
        # Keep a dict that has a list of processes by connection
        connections = dict()
        for connection in configuration['connections']:
            connections[connection] = list()
        return {'connections': connections,
                'min': configuration.get('min', self.MIN_CONSUMERS),
                'max': configuration.get('max', self.MAX_CONSUMERS),
                'last_proc_num': 0,
                'queue': configuration['queue'],
                'processes': dict()}

    def consumer_keyword(self, counts):
        """Return consumer or consumers depending on the process count.

        :param dict counts: The count dictionary to use process count
        :rtype: str

        """
        LOGGER.debug('Received %r', counts)
        return 'consumer' if counts['processes'] == 1 else 'consumers'

    def consumer_stats_counter(self):
        """Return a new consumer stats counter instance.

        :rtype: dict

        """
        return {process.Process.ERROR: 0,
                process.Process.PROCESSED: 0,
                process.Process.REDELIVERED: 0,
                process.Process.TIME_SPENT: 0,
                process.Process.TIME_WAITED: 0}

    def collect_results(self, data_values):
        """Receive the data from the consumers polled and process it.

        :param dict data_values: The poll data returned from the consumer
        :type data_values: dict

        """
        self.last_poll_results['timestamp'] = self.poll_data['timestamp']

        # Get the name and consumer name and remove it from what is reported
        consumer_name = data_values['consumer_name']
        del data_values['consumer_name']
        process_name = data_values['name']
        del data_values['name']

        # Add it to our last poll global data
        if consumer_name not in self.last_poll_results:
            self.last_poll_results[consumer_name] = dict()
        self.last_poll_results[consumer_name][process_name] = data_values

        # Find the position to remove the consumer from the list
        try:
            position = self.poll_data['processes'].index(process_name)
        except ValueError:
            LOGGER.error('Poll data from unexpected process: %s', process_name)
            position = None

        # Remove the consumer from the list we're waiting for data from
        if position is not None:
            self.poll_data['processes'].pop(position)

        # Calculate global stats
        if self.poll_data['processes']:
            LOGGER.debug('Still waiting on %i processes for stats',
                         len(self.poll_data['processes']))
            return

        # Calculate the stats
        self.stats = self.calculate_stats(self.last_poll_results)

        # If stats logging is enabled, log the stats
        if self.log_stats_enabled:
            self.log_stats()

    def kill_processes(self):
        """Gets called on shutdown by the timer when too much time has gone by,
        calling the terminate method instead of nicely asking for the consumers
        to stop.

        """
        LOGGER.critical('Max shutdown exceeded, forcibly exiting')

        for process in multiprocessing.active_children():
            process.terminate()
        LOGGER.info('All processes should have terminated')
        time.sleep(0.5)
        while multiprocessing.active_children():
            for process in multiprocessing.active_children():
                LOGGER.warning('Killing %s (%s)', process.name, process.pid)
                os.kill(int(process.pid), signal.SIGKILL)
            time.sleep(1)

        return self.set_state(self.STATE_STOPPED)

    def log_stats(self):
        """Output the stats to the LOGGER."""
        LOGGER.info('%i total %s have processed %i  messages with %i '
                    'errors, waiting %.2f seconds and have spent %.2f seconds '
                    'processing messages with an overall velocity of %.2f '
                    'messages per second.',
                    self.stats['counts']['processes'],
                    self.consumer_keyword(self.stats['counts']),
                    self.stats['counts']['processed'],
                    self.stats['counts']['failed'],
                    self.stats['counts']['idle_time'],
                    self.stats['counts']['processing_time'],
                    self.calculate_velocity(self.stats['counts']))
        for key in self.stats['consumers'].keys():
            LOGGER.info('%i %s for %s have processed %i messages with %i '
                        'errors, waiting %.2f seconds and have spent %.2f '
                        'seconds processing messages with an overall velocity '
                        'of %.2f messages per second.',
                        self.stats['consumers'][key]['processes'],
                        self.consumer_keyword(self.stats['consumers'][key]),
                        key,
                        self.stats['consumers'][key]['processed'],
                        self.stats['consumers'][key]['failed'],
                        self.stats['consumers'][key]['idle_time'],
                        self.stats['consumers'][key]['processing_time'],
                        self.calculate_velocity(self.stats['consumers'][key]))
        if self.poll_data['processes']:
            LOGGER.warning('%i process(es) did not respond with stats in '
                           'time: %r',
                           len(self.poll_data['processes']),
                           self.poll_data['processes'])

    def new_process(self, consumer_name, connection_name):
        """Create a new consumer instances

        :param str consumer_name: The name of the consumer
        :param str connection_name: The name of the connection
        :return tuple: (str, process.Process)

        """
        process_name = '%s_%s' % (consumer_name,
                                  self.new_process_number(consumer_name))
        LOGGER.debug('Creating a new process for %s: %s',
                     connection_name, process_name)
        kwargs = {'config': self.config,
                  'connection_name': connection_name,
                  'consumer_name': consumer_name,
                  'logging_config': self.logging_config,
                  'profile': self.profile,
                  'stats_queue': self.stats_queue}
        return process_name, process.Process(name=process_name, kwargs=kwargs)

    def new_process_number(self, name):
        """Increment the counter for the process id number for a given consumer
        configuration.

        :param str name: Consumer name
        :rtype: int

        """
        self.consumers[name]['last_proc_num'] += 1
        return self.consumers[name]['last_proc_num']

    def poll(self):
        """Start the poll process by invoking the get_stats method of the
        consumers. If we hit this after another interval without fully
        processing, note it with a warning.

        """
        # Check to see if we have outstanding things to poll
        #if self.poll_data['processes']:
        #    LOGGER.warn('Poll interval failure for consumer(s): %r',
        #                 ', '.join(self.poll_data['processes']))

        # Keep track of running consumers
        dead_processes = list()

        # Start our data collection dict
        self.poll_data = {'timestamp': time.time(),
                          'processes': list()}

        # Iterate through all of the consumers
        for process in self.active_processes:
            LOGGER.debug('Checking runtime state of %s', process.name)
            if process == multiprocessing.current_process():
                LOGGER.debug('Matched current process in active_processes')
                continue

            if not process.is_alive():
                LOGGER.critical('Found dead consumer %s', process.name)
                dead_processes.append(process.name)
            else:
                LOGGER.debug('Asking %s for stats', process.name)
                #self.poll_data['processes'].append(process.name)
                os.kill(process.pid, signal.SIGPROF)

        # Remove the objects if we have them
        for process_name in dead_processes:
            self.remove_process(process_name)

        # Check if we need to start more processes
        self.check_consumer_process_counts()

        # If we don't have any active consumers, shutdown
        if not self.total_process_count:
            LOGGER.debug('Did not find any active consumers in poll')
            return self.set_state(self.STATE_STOPPED)

        # Check to see if any consumers reported back and start timer if not
        #self.poll_results_check()

        # Start the timer again
        self.start_poll_timer()

    @property
    def poll_duration_exceeded(self):
        """Return true if the poll time has been exceeded.
        :rtype: bool

        """
        return (time.time() -
                self.poll_data['timestamp']) >= self.poll_interval

    def poll_results_check(self):
        """Check the polling results by checking to see if the stats queue is
        empty. If it is not, try and collect stats. If it is set a timer to
        call ourselves in _POLL_RESULTS_INTERVAL.

        """
        LOGGER.debug('Checking for poll results')
        results = True
        while results:
            try:
                stats = self.stats_queue.get(False)
            except Queue.Empty:
                LOGGER.debug('Stats queue is empty')
                break
            LOGGER.debug('Received stats from %s', stats['name'])
            self.collect_results(stats)

        # If there are pending consumers to get stats for, start the timer
        if self.poll_data['processes']:

            if self.poll_duration_exceeded and self.log_stats_enabled:
                self.log_stats()
            LOGGER.debug('Starting poll results timer for %i consumer(s)',
                         len(self.poll_data['processes']))
            self.start_poll_results_timer()

    def process(self, consumer_name, process_name):
        """Return the process handle for the given consumer name and process
        name.

        :param str consumer_name: The consumer name from config
        :param str process_name: The automatically assigned process name
        :rtype: rejected.process.Process

        """
        return self.consumers[consumer_name]['processes'][process_name]

    def process_count(self, name, connection):
        """Return the process count for the given consumer name and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :rtype: int

        """
        return len(self.consumers[name]['connections'][connection])

    def process_count_by_consumer(self, name):
        """Return the process count by consumer only.

        :param str name: The consumer name
        :rtype: int

        """
        count = 0
        for connection in self.consumers[name]['connections']:
            count += len(self.consumers[name]['connections'][connection])
        return count

    def process_spawn_qty(self, name, connection):
        """Return the number of processes to spawn for the given consumer name
        and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :rtype: int

        """
        return self.consumers[name]['min'] - self.process_count(name,
                                                                connection)

    def remove_process(self, process):
        """Remove the specified consumer process

        :param str process: The process name to remove

        """
        for consumer_name in self.consumers:
            if process in self.consumers[consumer_name]['processes']:
                del self.consumers[consumer_name]['processes'][process]
                self.remove_process_from_list(consumer_name, process)
                LOGGER.debug('Removed %s from %s\'s process list',
                             process, consumer_name)
                return
        LOGGER.warning('Could not find process %s to remove', process)

    def remove_process_from_list(self, name, process):
        """Remove the process from the consumer connections list.

        :param str name: The consumer name
        :param str process: The process name

        """
        for conn in self.consumers[name]['connections']:
            if process in self.consumers[name]['connections'][conn]:
                idx = self.consumers[name]['connections'][conn].index(process)
                self.consumers[name]['connections'][conn].pop(idx)
                LOGGER.debug('Process %s removed from %s connections',
                             process, name)
                return
        LOGGER.warning('Could not find %s in %s\'s process list', process, name)

    def set_process_name(self):
        """Set the process name for the top level process so that it shows up
        in logs in a more trackable fashion.

        """
        process = multiprocessing.current_process()
        for offset in xrange(0, len(sys.argv)):
            if sys.argv[offset] == '-c':
                name = sys.argv[offset + 1].split('/')[-1]
                process.name = name.split('.')[0]
                break


    def setup_consumers(self):
        """Iterate through each consumer in the configuration and kick off the
        minimal amount of processes, setting up the runtime data as well.

        """
        for name in self.config['Consumers']:

            # Hold the config as a shortcut
            config = self.config['Consumers'][name]

            # If queue is not configured, report the error but skip processes
            if 'queue' not in config:
                LOGGER.critical('Consumer %s is missing a queue, skipping',
                                name)
                continue

            # Create the dictionary values for this process
            self.consumers[name] = self.consumer_dict(config)

            # Iterate through the connections to create new consumer processes
            LOGGER.info('Starting %i consumers for %s',
                        self.consumers[name]['min'], name)
            for connection in self.consumers[name]['connections']:
                self.start_processes(name, connection,
                                     self.consumers[name]['min'])

    def start_poll_results_timer(self):
        """Start the poll results timer to see if there are results from the
        last poll yet.

        """
        self.results_timer = self.start_timer(self.results_timer,
                                              'poll_results_timer',
                                              self.POLL_RESULTS_INTERVAL,
                                              self.poll_results_check)

    def start_poll_timer(self):
        """Start the poll timer to fire the polling at the next interval"""
        self.poll_timer = self.start_timer(self.poll_timer, 'poll_timer',
                                           self.poll_interval, self.poll)

    def start_process(self, name, connection):
        """Start a new consumer process for the given consumer & connection name

        :param str name: The consumer name
        :param str connection: The connection name

        """
        LOGGER.info('Spawning new consumer process for %s to %s',
                    name, connection)

        # Create the new consumer process
        process_name, process = self.new_process(name, connection)

        # Append the process to the consumer process list
        self.consumers[name]['processes'][process_name] = process
        self.consumers[name]['connections'][connection].append(process_name)

        # Start the process
        process.start()

    def start_processes(self, name, connection, quantity):
        """Start the specified quantity of consumer processes for the given
        consumer and connection.

        :param str name: The consumer name
        :param str connection: The connection name
        :param int quantity: The quantity of processes to start

        """
        for process in xrange(0, quantity):
            self.start_process(name, connection)

    def start_timer(self, timer, name, duration, callback):
        """Start a timer for the given object, name, duration and callback.

        :param threading.Timer timer: The previous timer instance
        :param str name: The timer name
        :param int or float duration: The timer duration
        :param method callback: The method to call when timer fires
        :rtype: threading.Timer

        """
        if timer and timer.is_alive():
            timer.cancel()
            LOGGER.debug('Cancelled live timer: %s', name)

        timer = threading.Timer(duration, callback)
        timer.name = name
        timer.start()
        LOGGER.debug('Started %s timer for %.2f seconds calling back %r',
                     name, duration, callback)
        return timer

    def strip_unused_consumers(self):
        """Remove consumers that are not used from the configuration file if
        a specific consumer was specified in the cli options.

        """
        consumers = self.config['Consumers'].keys()
        for consumer in consumers:
            if consumer != self.consumer:
                LOGGER.debug('Removing %s for %s only processing',
                             consumer, self.consumer)
                del self.config['Consumers'][consumer]

    def stop_process(self, process):
        """Stop the specified process

        :param multiprocessing.Process process: The process to stop

        """
        if not process.is_alive():
            return

        LOGGER.debug('Sending signal to %s (%i) to stop',
                     process.name, process.pid)
        os.kill(int(process.pid), signal.SIGABRT)

    def stop_timers(self):
        """Stop all the active timeouts."""
        if self.poll_timer and self.poll_timer.is_alive():
            LOGGER.debug('Stopping the poll timer')
            self.poll_timer.cancel()

        if self.results_timer and self.results_timer.is_alive():
            LOGGER.debug('Stopping the poll results timer')
            self.results_timer.cancel()

    def validate_configuration(self):
        """Ensure that specific configuration sections exist in the config
        file.

        :rtype: bool

        """
        # Ensure that "Consumers" is a top-level configuration item
        if 'Consumers' not in self.config:
            LOGGER.error('Missing Consumers section of configuration, '
                         'aborting: %r', self.config)
            return False
        return True

    def stop_processes(self):
        """Iterate through all of the consumer processes shutting them down."""
        self.set_state(self.STATE_SHUTTING_DOWN)
        LOGGER.debug('Stopping consumer processes')
        self.stop_timers()

        active_processes = multiprocessing.active_children()

        # Stop if we have no running consumers
        if not active_processes:
            LOGGER.info('All consumer processes have stopped')
            return self.set_state(self.STATE_STOPPED)

        # Iterate through all of the bindings and try and shutdown processes
        for process in active_processes:
            self.stop_process(process)

        iterations = 0
        while multiprocessing.active_children():
            LOGGER.debug('Waiting on %i active processes to shut down',
                         self.total_process_count)
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                LOGGER.info('Caught CTRL-C, Killing Children')
                self.kill_processes()
                self.set_state(self.STATE_STOPPED)
                return

            iterations += 1

            # If the shutdown process waited long enough, kill the consumers
            if iterations == self.MAX_SHUTDOWN_WAIT:
                self.kill_processes()
                break

        LOGGER.debug('All consumer processes stopped')
        self.set_state(self.STATE_STOPPED)

    def run(self):
        """When the consumer is ready to start running, kick off all of our
        consumer consumers and then loop while we process messages.

        """
        # Set the state to active
        self.set_state(self.STATE_ACTIVE)

        # Get the consumer section from the config
        if not self.validate_configuration():
            LOGGER.error('Configuration format did not validate')
            return self.set_state(self.STATE_STOPPED)

        # Strip consumers if a consumer is specified
        if self.consumer:
            self.strip_unused_consumers()

        # Setup consumers and start the processes
        self.setup_consumers()

        # Kick off the poll timer
        self.start_poll_timer()

        # Loop for the lifetime of the app, sleeping 0.2 seconds at a time
        while self.is_running:
            time.sleep(0.2)

        # Note we're exiting run
        LOGGER.info('Exiting Master Control Program')

    @property
    def total_process_count(self):
        """Returns the active consumer process count

        :rtype: int

        """
        return len(self.active_processes)
