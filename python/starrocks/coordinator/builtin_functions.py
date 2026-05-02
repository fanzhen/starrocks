"""Built-in functions for Daft Coordinator testing and demos."""

from __future__ import annotations


def identity_transform(daft_df):
    """Identity function — returns the DataFrame unchanged.

    Useful for E2E testing of the map_batches pipeline.
    """
    return daft_df
