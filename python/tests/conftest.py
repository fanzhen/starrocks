"""Shared pytest fixtures."""

import os
import pytest


def sr_host() -> str:
    return os.getenv("SR_HOST", "127.0.0.1")


def sr_port() -> int:
    return int(os.getenv("SR_PORT", "9030"))
