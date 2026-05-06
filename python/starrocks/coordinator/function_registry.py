"""Thread-safe registry for Python functions used in Daft map_batches."""

from __future__ import annotations

import importlib
import json
import logging
import os
import threading
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)

_DEFAULT_PERSIST_DIR = os.path.expanduser("~/.starrocks/coordinator")
_PERSIST_FILENAME = "functions.json"


class FunctionRegistry:
    """Manages registered Python functions for Daft execution.

    Functions are loaded eagerly on registration (fail-fast) and
    looked up by name at execution time. Optionally persists
    registrations to a JSON file so they survive restarts.
    """

    def __init__(self, persist_dir: str | None = _DEFAULT_PERSIST_DIR) -> None:
        self._lock = threading.Lock()
        self._functions: dict[str, Callable] = {}
        self._metadata: dict[str, str] = {}  # name -> "module.callable"
        self._persist_path: Path | None = None
        if persist_dir is not None:
            self._persist_path = Path(persist_dir) / _PERSIST_FILENAME

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
        self._save_persisted()

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
        self._save_persisted()

    def list_functions(self) -> dict[str, str]:
        """Return {name: 'module.callable'} for all registered functions."""
        with self._lock:
            return dict(self._metadata)

    def load_persisted(self) -> int:
        """Load previously persisted function registrations from JSON.

        Returns the number of functions successfully restored.
        Functions that fail to import are skipped with a warning.
        """
        if self._persist_path is None or not self._persist_path.exists():
            return 0
        try:
            data = json.loads(self._persist_path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("Failed to read persisted functions from %s: %s",
                           self._persist_path, e)
            return 0

        functions = data.get("functions", {})
        restored = 0
        for name, info in functions.items():
            module_path = info.get("module_path", "")
            callable_name = info.get("callable_name", "")
            try:
                self.register(name, module_path, callable_name)
                restored += 1
            except Exception as e:
                logger.warning("Skipping persisted function %r (%s.%s): %s",
                               name, module_path, callable_name, e)
        logger.info("Restored %d/%d persisted functions from %s",
                     restored, len(functions), self._persist_path)
        return restored

    def _save_persisted(self) -> None:
        """Write current registrations to the JSON persistence file."""
        if self._persist_path is None:
            return
        with self._lock:
            funcs = {}
            for name, meta in self._metadata.items():
                parts = meta.rsplit(".", 1)
                funcs[name] = {
                    "module_path": parts[0],
                    "callable_name": parts[1] if len(parts) > 1 else meta,
                }
        try:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            self._persist_path.write_text(
                json.dumps({"functions": funcs}, indent=2) + "\n",
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning("Failed to persist functions to %s: %s",
                           self._persist_path, e)

    def __len__(self) -> int:
        with self._lock:
            return len(self._functions)
