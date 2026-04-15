"""Backend for listening to local events. Uses Watchdog."""

import fnmatch
import os
from contextlib import contextmanager
from functools import partial
from queue import Queue

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from pytroll_watchers.publisher import parse_metadata


@contextmanager
def listen_to_local_events(directory, file_pattern=None, observer_type="os"):
    """Listen to local events.

    This context manager returns a generator producing filenames that are detected locally.

    Args:
        directory: The directory to watch for changes.
        file_pattern: the (trollsift) pattern to use globbing the events and filter them.
        observer_type: how to watch for events ("os" or "minio")

    Yields:
        A generator of filenames.
    """
    if file_pattern is None:
        yield ((event, dict()) for event in generate_local_events(directory, observer_type))
    else:
        if isinstance(file_pattern, str):
            file_patterns = [file_pattern]
        else:
            file_patterns = file_pattern
        yield generate_events_with_metadata(directory, file_patterns, observer_type)


def generate_events_with_metadata(directory, file_patterns, observer_type):
    """Generate tuples of (event, metadata)."""
    for filename in generate_local_events(directory, observer_type):
        for pattern in file_patterns:
            try:
                file_metadata = parse_metadata(os.path.join(directory, pattern), filename)
            except ValueError:
                continue
            else:
                yield filename, file_metadata
                break


def generate_local_events(directory, observer_type):
    """Generate local events."""
    queue = Queue()

    if observer_type == "os":
        obs = _create_watchdog_os_observer(directory, queue)
    elif observer_type == "polling":
        obs = _create_watchdog_polling_observer(directory, queue)
    else:
        raise ValueError("Observer type can be either 'os' or 'polling'.")

    obs.start()
    try:
        yield from _iterate_over_queue(queue)
    finally:
        obs.stop()


def _create_watchdog_polling_observer(directory, queue, timeout=1.0):
    """Create a watchdog polling observer on directory.

    Args:
        directory: the directory to watch for events.
        queue: the queue to append events to.
        timeout: the timeout to use for polling, in seconds.

    Returns:
        The instanciated observer object.
    """
    observer_class = partial(PollingObserver, timeout=timeout)
    handler_class = _WatchdogCreationHandler
    return _create_watchdog_observer(directory, queue, observer_class, handler_class)


def _create_watchdog_os_observer(directory, queue, timeout=1.0):
    """Create a watchdog os-dependent observer on directory.

    Args:
        directory: the directory to watch for events.
        queue: the queue to append events to.
        timeout: the timeout to use for detecting, in seconds.

    Returns:
        The instanciated observer object.
    """
    observer_class = partial(Observer, timeout=timeout, generate_full_events=True)
    handler_class = _WatchdogChangeHandler
    return _create_watchdog_observer(directory, queue, observer_class, handler_class)


def _iterate_over_queue(queue):
    """Iterate over the queue.

    This is it's own function so that it can be mocked during tests.
    """
    return iter(queue.get, None)


def _create_watchdog_observer(directory, queue, observer_class, handler_class):
    """Create a watchdog observer.

    Args:
        directory: the directory to watch for events.
        queue: the queue to append events to.
        observer_class: the class to instanciate as observer.
        handler_class: the class to use as handler.

    Returns:
        The instanciated observer object.
    """
    observer = observer_class()

    def function_to_run(filename):
        queue.put(filename)

    glob_pattern = "*"

    handler = handler_class(function_to_run, os.path.join(directory, glob_pattern))

    observer.schedule(handler, directory, recursive=True)

    return observer


class _WatchdogHandler(FileSystemEventHandler):
    """Trigger processing on filesystem events, with filename matching."""

    def __init__(self, fun, pattern=None):
        """Initialize the processor."""
        super().__init__()
        self.fun = fun
        self.pattern = pattern

    def dispatch(self, event):
        """Dispatches events to the appropriate methods."""
        if self.pattern is None:
            return super().dispatch(event)
        if event.is_directory:
            return
        if getattr(event, "dest_path", None):
            pathname = os.fsdecode(event.dest_path)
        elif event.src_path:
            pathname = os.fsdecode(event.src_path)
        if fnmatch.fnmatch(pathname, self.pattern):
            super().dispatch(event)


class _WatchdogChangeHandler(_WatchdogHandler):
    """Trigger processing on filesystem events that change a file (moving, close (write))."""

    def on_closed(self, event):
        """Process file closed."""
        self.fun(event.src_path)

    def on_moved(self, event):
        """Process a file being moved to the destination directory."""
        self.fun(event.dest_path)


class _WatchdogCreationHandler(_WatchdogHandler):
    """Trigger processing on filesystem events that create a file (moving, creation)."""

    def on_created(self, event):
        """Process file closing."""
        self.fun(event.src_path)

    def on_moved(self, event):
        """Process a file being moved to the destination directory."""
        self.fun(event.dest_path)
