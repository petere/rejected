"""Wraps a worker process with a mechanism for it to increment stats via a
multiprocessing.Connection.

"""
import logging
import multiprocessing
import os
import psutil

import signal
import threading
import time

from rejected.worker import process

LOGGER = logging.getLogger(__name__)


class WorkerThread(threading.Thread):
    """The WorkerThread object is used to carry information about each child
    process, the worker that is running, and stats about its performance.

    If a time to live (ttl) is set in the configuration, the health check timer
    will stop the child process after the ttl has expired.

    """
    ACK = 'ack'
    COMPLETE = 'complete'
    ERROR = 'errors'
    EXCEPTION = 'exception'
    REDELIVERED = 'redelivered'
    REJECT = 'reject'
    START = 'start'

    HEATH_CHECK_INTERVAL = 5
    KILL_INTERVAL = 3
    MAX_EXCEPTIONS = 5

    POLL_INTERVAL = 1

    # Default message pre-allocation value
    QOS_PREFETCH_COUNT = 1
    QOS_PREFETCH_MULTIPLIER = 1.25
    QOS_MAX = 10000

    def __init__(self, group=None, target=None, name=None, args=(),
                 kwargs=None):
        if kwargs is None:
            kwargs = {}
        super(WorkerThread, self).__init__(group, target, name, args, kwargs)

        self._busy_time = 0
        self._config = kwargs.get('worker_config')
        self._connection_config = kwargs.get('connection_config')
        self._logging_config = kwargs.get('logging_config')
        self._errors = 0
        self._exceptions = 0
        self._idle_start = time.time()
        self._idle_time = 0
        self._max_exceptions = self._config.get('max_exceptions',
                                                self.MAX_EXCEPTIONS)
        self._msgs_acked = 0
        self._msgs_processed = 0
        self._msgs_redelivered = 0
        self._msgs_rejected = 0
        self._msg_start = None
        self._name = name
        self._qos_prefetch_count = self.QOS_PREFETCH_COUNT
        self._timer = None

        self._parent_pipe, self._child_pipe = self._get_stats_pipe()
        self._worker = self._create_worker()
        self._psutil = None

        self._start_time = time.time()
        self._worker_type = kwargs.get('worker_type')

        if self._config.get('ttl'):
            self._deadline = time.time() + self._config.get('ttl')

    def kill(self):
        """Kill the child process."""
        if not self._worker.is_alive():
            LOGGER.info('Worker is already dead, not killing')
            return
        self._send_signal(signal.SIGKILL)

    @property
    def memory_usage(self):
        """Return the memory utilization of the child process

        :rtype: psutil._common.meminfo

        """
        return self._psutil.get_memory_info()

    def run(self):
        """Start the child process"""
        self._worker.start()
        if not self._worker.is_alive():
            LOGGER.critical('Child process could not start')
            return

        self._psutil = psutil.Process(self._worker.pid)
        self._start_health_check_timer()
        while self._worker.is_alive():
            if self._parent_pipe.poll(self.POLL_INTERVAL):
                self._process_worker_data(self._parent_pipe.recv())
        LOGGER.debug('Exiting run')

    def stats(self):
        """Return a stats dictionary for the worker

        :rtype: dict

        """
        memory_use = self.memory_usage
        return {'memory_rss': memory_use.rss,
                'memory_vms': memory_use.vms,
                'busy_time': self._busy_time,
                'idle_time': self._idle_time,
                'exceptions': self._exceptions,
                'msgs_acked': self._msgs_acked,
                'msgs_rejected': self._msgs_rejected}

    def stop(self):
        """Tell the worker process to stop by sending SIGABRT"""
        if not self._worker.is_alive():
            if hasattr(self, '_parent_pipe'):
                del self._parent_pipe
            LOGGER.info('Worker is already stopped, not stopping')
            return
        self._send_signal(signal.SIGABRT)

    def terminate(self):
        """Terminate the child process, sending a SIGTERM"""
        if not self._worker.is_alive():
            LOGGER.info('Worker is already stopped, not terminating')
            return
        self._send_signal(signal.SIGTERM)

    def _check_exception_count(self):
        """Checks to see if the worker has had too many exceptions and if so,
        requests a stop. Will return True if too many exceptions have taken
        place.

        :rtype: bool

        """
        if self._exceptions >= self._max_exceptions:
            LOGGER.warning('%s received too many errors (%i), requesting stop',
                           self._name, self._exceptions)
            self.stop()
            return True
        return False

    def _create_worker(self):
        """Create a worker process, returning the handle for the process"""
        kwargs = {'config': self._config,
                  'connection_config': self._connection_config,
                  'logging_config': self._logging_config,
                  'pipe': self._child_pipe}
        return process.WorkerProcess(name=self._name, kwargs=kwargs)

    def _get_stats_pipe(self):
        """Get a multiprocessing stats pipe.

        :rtype: tuple(multiprocessing.Connection, multiprocessing.Connection)

        """
        return multiprocessing.Pipe(True)

    def _on_heath_check_timer(self):
        """Called by the threading module when a timer has fired. It will check
        to make sure the quantity of exceptions has not met or exceeded the
        threshold. If the deadline has passed, it will start the shutdown
        process.

        """
        if self._check_exception_count():
            return self._start_kill_timer()

        if self._deadline and self._deadline <= time.time:
            LOGGER.debug('Deadline has passed for Worker TTL, shutting down')
            self.stop()

        self._start_health_check_timer()

    def _process_worker_data(self, value):
        """Data from the worker is passed up as a dictionary through a
        multiprocessing.Connection. This method processes the data and
        increments the appropriate counters. The MCP will poll for this data
        periodically.

        :param dict value: The value from the worker

        """
        event_name = value.get('event')
        if event_name == self.ACK:
            self._busy_time += time.time() - self._msg_start
            self._msgs_acked += 1
            self._msgs_processed += 1
        elif event_name == self.COMPLETE:
            self._busy_time += time.time() - self._msg_start
            self._msgs_processed += 1
        elif event_name == self.ERROR:
            self._errors += 1
        elif event_name == self.EXCEPTION:
            self._exceptions += 1
        elif event_name == self.REJECT:
            self._busy_time += time.time() - self._msg_start
            self._msgs_rejected += 1
            self._msgs_processed += 1
        elif event_name == self.REDELIVERED:
            self._msgs_redelivered += 1
        elif event_name == self.START:
            self._idle_time += time.time() - self._idle_start
            self._msg_start = time.time()
        else:
            LOGGER.critical('Unknown value type: %r', value)

    def _send_signal(self, signum):
        """Send a signal to the child process.

        :param int signum: The signal to send

        """
        os.kill(int(self._worker.pid), signum)

    def _start_health_check_timer(self):
        """Creates a new timer that will call on_health_check_timer when it
        fires.

        """
        self._timer = threading.Timer(self.HEATH_CHECK_INTERVAL,
                                      self._on_heath_check_timer)

    def _start_kill_timer(self):
        """Start a timer that will call self.kill when it has expired, if it
        is not stopped.

        """
        if not self._worker.is_alive():
            LOGGER.debug('Process is not dead, not starting kill timer')
            return
        self._timer = threading.Timer(self.KILL_INTERVAL, self.kill)
