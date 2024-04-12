"""Main package file for pytroll watchers."""

from pytroll_watchers.local_watcher import file_generator as local_generator
from pytroll_watchers.local_watcher import file_publisher as local_publisher
from pytroll_watchers.minio_notification_watcher import file_generator as minio_generator
from pytroll_watchers.minio_notification_watcher import file_publisher as minio_publisher


def get_publisher_for_backend(backend):
    """Get the right publisher for the given backend.

    For the parameters to pass the returned function, check the individual backend documentation pages.

    Example:
        >>> file_publisher = get_publisher_for_backend("local")
        >>> file_publisher(fs_config, publisher_config, message_config)

    """
    if backend == "minio":
        return minio_publisher
    elif backend == "local":
        return local_publisher
    else:
        raise ValueError(f"Unknown backend {backend}.")

def get_generator_for_backend(backend):
    """Get the right generator for the given backend.

    For the parameters to pass the returned function, check the individual backend documentation pages.

    Example:
        >>> file_generator = get_generator_for_backend("local")
        >>> for filename, file_metadata in file_generator("/tmp"):
        ...     # do something with filename and file_metadata

    """
    if backend == "minio":
        return minio_generator
    elif backend == "local":
        return local_generator
    else:
        raise ValueError(f"Unknown backend {backend}.")
