"""Backend for listening to local events. Uses Watchdog."""

import fnmatch
import os
from contextlib import contextmanager, suppress
from functools import partial
from queue import Empty, Queue

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from pytroll_watchers.common import TERM_EVENT
from pytroll_watchers.publisher import parse_metadata


@contextmanager
def watch_local_directory(directory, observer_type="os", trigger="closed"):
    """Watch a local directory for new files.

    The watch is active as soon as the context is entered, so no file appearing afterwards is missed.

    Args:
        directory: The directory to watch for changes.
        observer_type: how to watch for events ("os" or "polling").
        trigger: what event announces a file with the "os" observer: "closed" (default) when it is closed after
            writing, or "created" as soon as it is created, possibly before it is completely written.

    Yields:
        A generator of the paths of the new files.
    """
    queue = Queue()
    observer = _create_observer(directory, queue, observer_type, trigger)
    observer.start()
    try:
        yield _iterate_over_queue(queue)
    finally:
        observer.stop()


def add_metadata(paths, directory, file_pattern=None):
    """Generate tuples of (path, metadata), skipping the paths that match none of the file patterns.

    Args:
        paths: The paths to add metadata to.
        directory: The directory the file patterns are relative to.
        file_pattern: The (trollsift) pattern, or list of patterns, to parse the metadata with. The first matching
            pattern is used. If None, all paths are kept, with empty metadata.
    """
    if file_pattern is None:
        yield from ((path, dict()) for path in paths)
        return
    if isinstance(file_pattern, str):
        file_pattern = [file_pattern]
    for path in paths:
        for pattern in file_pattern:
            try:
                file_metadata = parse_metadata(os.path.join(directory, pattern), path)
            except ValueError:
                continue
            else:
                yield path, file_metadata
                break


def _create_observer(directory, queue, observer_type, trigger):
    """Create the observer of type `observer_type` on directory."""
    if observer_type == "os":
        return _create_watchdog_os_observer(directory, queue, trigger)
    if observer_type == "polling":
        return _create_watchdog_polling_observer(directory, queue)
    raise ValueError("Observer type can be either 'os' or 'polling'.")


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


def _create_watchdog_os_observer(directory, queue, trigger, timeout=1.0):
    """Create a watchdog os-dependent observer on directory.

    Args:
        directory: the directory to watch for events.
        queue: the queue to append events to.
        trigger: the event announcing a file, "created" or "closed".
        timeout: the timeout to use for detecting, in seconds.

    Returns:
        The instanciated observer object.
    """
    try:
        handler_class = _OS_HANDLERS_BY_TRIGGER[trigger]
    except KeyError:
        raise ValueError("Trigger can be either 'created' or 'closed'.") from None
    observer_class = partial(Observer, timeout=timeout, generate_full_events=True)
    return _create_watchdog_observer(directory, queue, observer_class, handler_class)


def _iterate_over_queue(queue):
    """Iterate over the queue.

    This is it's own function so that it can be mocked during tests.
    """
    while not TERM_EVENT.is_set():
        with suppress(Empty):
            yield queue.get(timeout=1)


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
        """Process file creation."""
        self.fun(event.src_path)

    def on_moved(self, event):
        """Process a file being moved to the destination directory."""
        self.fun(event.dest_path)


_OS_HANDLERS_BY_TRIGGER = {"created": _WatchdogCreationHandler,
                           "closed": _WatchdogChangeHandler}
