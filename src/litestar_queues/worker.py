__all__ = ("Worker",)


class Worker:
    """Local worker placeholder for later runtime chapters."""

    __slots__ = ()

    async def start(self) -> None:
        """Start the local worker."""
        message = "The local worker runtime lands in Chapter 2."
        raise NotImplementedError(message)
