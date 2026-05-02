"""gRPC server implementation for the Daft Coordinator."""

from __future__ import annotations

import logging

import grpc

from starrocks.coordinator.daft_driver import DaftDriver
from starrocks.coordinator.function_registry import FunctionRegistry
from starrocks.coordinator.proto import coordinator_pb2, coordinator_pb2_grpc

logger = logging.getLogger(__name__)


class DaftCoordinatorServicer(coordinator_pb2_grpc.DaftCoordinatorServicer):
    """Implements the DaftCoordinator gRPC service."""

    def __init__(self, ray_address: str | None = None) -> None:
        self._ray_address = ray_address
        self._registry = FunctionRegistry()
        self._driver = DaftDriver(self._registry)
        self._init_ray(ray_address)

    def _init_ray(self, ray_address: str | None) -> None:
        """Initialize Ray and set Daft to use Ray runner."""
        try:
            import ray
            if not ray.is_initialized():
                if ray_address:
                    ray.init(address=ray_address)
                else:
                    ray.init()
            import daft
            daft.context.set_runner_ray()
            logger.info("Ray initialized (address=%s), Daft runner set to Ray", ray_address)
        except ImportError:
            logger.warning("Ray/Daft not available — running in local mode")
        except Exception as e:
            logger.warning("Failed to initialize Ray: %s — running in local mode", e)

    def SubmitDaftPlan(self, request, context):
        """Execute a Daft plan and stream back results (text format with Arrow IPC)."""
        request_id = request.request_id or "unknown"
        logger.info("[%s] SubmitDaftPlan: sql=%r, ops=%d",
                     request_id, request.source_sql, len(request.operations))
        try:
            col_names, rows = self._driver.execute_text(request)
            # Stream in batches of _TEXT_BATCH_SIZE rows
            batch_size = 4096
            total = len(rows)
            if total == 0:
                yield coordinator_pb2.DaftPlanResponse(
                    column_names=col_names,
                    row_values=[],
                    num_rows=0,
                    is_last=True,
                )
                return
            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)
                batch_rows = rows[start:end]
                # Flatten: each row's values concatenated
                flat_values = []
                for row in batch_rows:
                    flat_values.extend(row)
                yield coordinator_pb2.DaftPlanResponse(
                    column_names=col_names if start == 0 else [],
                    row_values=flat_values,
                    num_rows=len(batch_rows),
                    is_last=(end >= total),
                )
        except Exception as e:
            logger.exception("[%s] SubmitDaftPlan failed", request_id)
            yield coordinator_pb2.DaftPlanResponse(
                error=f"{type(e).__name__}: {e}",
                is_last=True,
            )

    def RegisterFunction(self, request, context):
        """Register a Python function for use in map_batches."""
        logger.info("RegisterFunction: %s -> %s.%s",
                     request.function_name, request.module_path, request.callable_name)
        try:
            self._registry.register(
                request.function_name,
                request.module_path,
                request.callable_name,
            )
            return coordinator_pb2.RegisterFunctionResponse(
                success=True,
                message=f"Registered {request.function_name}",
            )
        except Exception as e:
            logger.exception("RegisterFunction failed")
            return coordinator_pb2.RegisterFunctionResponse(
                success=False,
                message=f"{type(e).__name__}: {e}",
            )

    def GetStatus(self, request, context):
        """Return service status, registered function count, and Ray resources."""
        ray_resources: dict[str, str] = {}
        try:
            import ray
            if ray.is_initialized():
                for k, v in ray.cluster_resources().items():
                    ray_resources[k] = str(v)
        except Exception:
            pass

        return coordinator_pb2.StatusResponse(
            status="SERVING",
            registered_functions=len(self._registry),
            ray_resources=ray_resources,
        )

    @property
    def registry(self) -> FunctionRegistry:
        return self._registry
