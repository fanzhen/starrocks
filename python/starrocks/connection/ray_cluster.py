"""Ray cluster connection for distributed Daft execution."""

from __future__ import annotations


class RayConnection:
    """Manages a connection to a Ray cluster.

    Wraps ``ray.init(address=...)`` with health checking and resource queries.
    """

    def __init__(self, address: str) -> None:
        import ray
        self._address = address
        self._ray = ray
        if not ray.is_initialized():
            ray.init(address=address)

    def is_connected(self) -> bool:
        """Check if the Ray cluster is reachable."""
        return self._ray.is_initialized()

    def cluster_resources(self) -> dict:
        """Return available cluster resources (CPUs, GPUs, memory, etc.)."""
        return self._ray.cluster_resources()

    def shutdown(self) -> None:
        """Disconnect from the Ray cluster."""
        if self._ray.is_initialized():
            self._ray.shutdown()
