from typing import NoReturn

__all__ = ("task",)


def task(*args: object, **kwargs: object) -> NoReturn:
    """Task decorator placeholder.

    The public task decorator lands with the core queue API in Chapter 2.
    """
    message = "The task decorator lands in Chapter 2."
    raise NotImplementedError(message)
