"""Thread-safe registry for Python functions used in Daft map_batches."""

from __future__ import annotations

import importlib
import logging
import threading
from typing import Callable

logger = logging.getLogger(__name__)


class FunctionRegistry:
    """Manages registered Python functions for Daft execution.

    Functions are loaded eagerly on registration (fail-fast) and
    looked up by name at execution time.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._functions: dict[str, Callable] = {}
        self._metadata: dict[str, str] = {}  # name -> "module.callable"

    def register(self, name: str, module_path: str, callable_name: str) -> None:
        """Register a function by importing it immediately.

        Args:
            name: Logical name for lookup (e.g. "clip_embed").
            module_path: Python module path (e.g. "mymodule.transforms").
            callable_name: Attribute name in the module (e.g. "clip_embed").

        Raises:
            ImportError: If the module cannot be imported.
            AttributeError: If the callable is not found in the module.
            TypeError: If the attribute is not callable.
        """
        mod = importlib.import_module(module_path)
        func = getattr(mod, callable_name)
        if not callable(func):
            raise TypeError(
                f"{module_path}.{callable_name} is not callable"
            )
        with self._lock:
            self._functions[name] = func
            self._metadata[name] = f"{module_path}.{callable_name}"
        logger.info("Registered function %r -> %s.%s", name, module_path, callable_name)

    def get(self, name: str) -> Callable:
        """Look up a registered function by name.

        Raises:
            KeyError: If the function is not registered.
        """
        with self._lock:
            try:
                return self._functions[name]
            except KeyError:
                raise KeyError(f"Function not registered: {name!r}")

    def unregister(self, name: str) -> None:
        """Remove a function from the registry."""
        with self._lock:
            self._functions.pop(name, None)
            self._metadata.pop(name, None)

    def list_functions(self) -> dict[str, str]:
        """Return {name: 'module.callable'} for all registered functions."""
        with self._lock:
            return dict(self._metadata)

    def __len__(self) -> int:
        with self._lock:
            return len(self._functions)
