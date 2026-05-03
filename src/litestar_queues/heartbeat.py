__all__ = ("Heartbeat",)


class Heartbeat:
    """Backend-agnostic heartbeat placeholder."""

    __slots__ = ()

    async def start(self) -> None:
        """Start heartbeat tracking."""
        message = "Heartbeat behavior lands in a later backend chapter."
        raise NotImplementedError(message)
