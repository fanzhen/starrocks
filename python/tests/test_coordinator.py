"""Unit tests for the Daft Coordinator (mock-based, no Ray/StarRocks required)."""

from __future__ import annotations

import importlib.util
import math
from unittest import mock

import pytest

from starrocks.coordinator.function_registry import FunctionRegistry

# Daft may not be available (e.g. Python 3.14+). Check without importing
# (import can cause SIGILL on unsupported platforms).
_daft_available = bool(importlib.util.find_spec("daft"))

requires_daft = pytest.mark.skipif(
    not _daft_available,
    reason="daft not installed or not available on this Python version",
)


# ---------------------------------------------------------------------------
# FunctionRegistry tests
# ---------------------------------------------------------------------------

class TestFunctionRegistry:
    def test_register_and_get(self):
        reg = FunctionRegistry()
        reg.register("double_it", "math", "sqrt")
        func = reg.get("double_it")
        assert func is math.sqrt

    def test_not_found_raises_key_error(self):
        reg = FunctionRegistry()
        with pytest.raises(KeyError, match="not_registered"):
            reg.get("not_registered")

    def test_bad_module_raises_import_error(self):
        reg = FunctionRegistry()
        with pytest.raises(ImportError):
            reg.register("bad", "nonexistent_module_xyz_123", "func")

    def test_bad_callable_raises_attribute_error(self):
        reg = FunctionRegistry()
        with pytest.raises(AttributeError):
            reg.register("bad", "math", "nonexistent_attr_xyz_123")

    def test_not_callable_raises_type_error(self):
        reg = FunctionRegistry()
        with pytest.raises(TypeError, match="not callable"):
            reg.register("pi_val", "math", "pi")

    def test_unregister(self):
        reg = FunctionRegistry()
        reg.register("f", "math", "sqrt")
        reg.unregister("f")
        with pytest.raises(KeyError):
            reg.get("f")

    def test_list_functions(self):
        reg = FunctionRegistry()
        reg.register("a", "math", "sqrt")
        reg.register("b", "math", "ceil")
        funcs = reg.list_functions()
        assert funcs == {"a": "math.sqrt", "b": "math.ceil"}

    def test_len(self):
        reg = FunctionRegistry()
        assert len(reg) == 0
        reg.register("f", "math", "sqrt")
        assert len(reg) == 1


# ---------------------------------------------------------------------------
# DaftDriver tests (mocked, require daft)
# ---------------------------------------------------------------------------

@requires_daft
class TestDaftDriver:
    def test_execute_simple(self):
        """Test DaftDriver.execute with mocked Arrow Flight + Daft."""
        import pyarrow as pa
        from starrocks.coordinator.daft_driver import DaftDriver
        from starrocks.coordinator.proto import coordinator_pb2

        reg = FunctionRegistry()

        def double_values(daft_df):
            import daft
            return daft_df.with_column(
                "v",
                daft.col("v") * 2,
            )

        reg._functions["double_values"] = double_values
        reg._metadata["double_values"] = "test.double_values"

        driver = DaftDriver(reg)

        request = coordinator_pb2.DaftPlanRequest(
            request_id="test-1",
            arrow_flight_endpoint="grpc://localhost:9408",
            source_sql="SELECT 1 as v, 2 as w",
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(function_name="double_values"),
                ),
            ],
        )

        table = pa.table({"v": [1, 2, 3], "w": [10, 20, 30]})
        with mock.patch.object(driver, "_fetch_source_data", return_value=table):
            batches = list(driver.execute(request))

        assert len(batches) >= 1
        result_table = _decode_ipc_batches(batches)
        assert result_table.num_rows == 3
        assert result_table.column("v").to_pylist() == [2, 4, 6]
        assert result_table.column("w").to_pylist() == [10, 20, 30]

    def test_execute_filter_and_limit(self):
        """Test filter + limit operations."""
        import pyarrow as pa
        from starrocks.coordinator.daft_driver import DaftDriver
        from starrocks.coordinator.proto import coordinator_pb2
        import json

        reg = FunctionRegistry()
        driver = DaftDriver(reg)

        request = coordinator_pb2.DaftPlanRequest(
            request_id="test-2",
            arrow_flight_endpoint="grpc://localhost:9408",
            source_sql="SELECT 1",
            operations=[
                coordinator_pb2.DaftOperation(
                    filter=coordinator_pb2.FilterOp(
                        expr_json=json.dumps({"col": "v", "op": ">", "value": 2}),
                    ),
                ),
                coordinator_pb2.DaftOperation(
                    limit=coordinator_pb2.LimitOp(count=1),
                ),
            ],
        )

        table = pa.table({"v": [1, 2, 3, 4, 5]})
        with mock.patch.object(driver, "_fetch_source_data", return_value=table):
            batches = list(driver.execute(request))

        result_table = _decode_ipc_batches(batches)
        assert result_table.num_rows == 1
        assert result_table.column("v").to_pylist()[0] > 2

    def test_execute_projection(self):
        """Test projection operation."""
        import pyarrow as pa
        from starrocks.coordinator.daft_driver import DaftDriver
        from starrocks.coordinator.proto import coordinator_pb2

        reg = FunctionRegistry()
        driver = DaftDriver(reg)

        request = coordinator_pb2.DaftPlanRequest(
            request_id="test-3",
            arrow_flight_endpoint="grpc://localhost:9408",
            source_sql="SELECT 1",
            operations=[
                coordinator_pb2.DaftOperation(
                    projection=coordinator_pb2.ProjectionOp(columns=["a"]),
                ),
            ],
        )

        table = pa.table({"a": [1, 2], "b": [3, 4]})
        with mock.patch.object(driver, "_fetch_source_data", return_value=table):
            batches = list(driver.execute(request))

        result_table = _decode_ipc_batches(batches)
        assert result_table.column_names == ["a"]
        assert result_table.column("a").to_pylist() == [1, 2]

    def test_unknown_function_raises(self):
        """Requesting an unregistered function should raise."""
        import pyarrow as pa
        from starrocks.coordinator.daft_driver import DaftDriver
        from starrocks.coordinator.proto import coordinator_pb2

        reg = FunctionRegistry()
        driver = DaftDriver(reg)

        request = coordinator_pb2.DaftPlanRequest(
            request_id="test-err",
            arrow_flight_endpoint="grpc://localhost:9408",
            source_sql="SELECT 1",
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(function_name="nope"),
                ),
            ],
        )

        table = pa.table({"v": [1]})
        with mock.patch.object(driver, "_fetch_source_data", return_value=table):
            with pytest.raises(KeyError, match="nope"):
                list(driver.execute(request))


# ---------------------------------------------------------------------------
# gRPC Servicer tests (mocked, no actual gRPC server)
# ---------------------------------------------------------------------------

class TestDaftCoordinatorServicer:
    def test_get_status(self):
        from starrocks.coordinator.server import DaftCoordinatorServicer
        from starrocks.coordinator.proto import coordinator_pb2

        with mock.patch("starrocks.coordinator.server.DaftCoordinatorServicer._init_ray"):
            servicer = DaftCoordinatorServicer(ray_address=None)

        ctx = mock.MagicMock()
        resp = servicer.GetStatus(coordinator_pb2.StatusRequest(), ctx)
        assert resp.status == "SERVING"
        assert resp.registered_functions == 0

    def test_register_function(self):
        from starrocks.coordinator.server import DaftCoordinatorServicer
        from starrocks.coordinator.proto import coordinator_pb2

        with mock.patch("starrocks.coordinator.server.DaftCoordinatorServicer._init_ray"):
            servicer = DaftCoordinatorServicer(ray_address=None)

        ctx = mock.MagicMock()
        resp = servicer.RegisterFunction(
            coordinator_pb2.RegisterFunctionRequest(
                function_name="my_sqrt",
                module_path="math",
                callable_name="sqrt",
            ),
            ctx,
        )
        assert resp.success is True
        assert "Registered" in resp.message
        assert len(servicer.registry) == 1

    def test_register_function_bad_module(self):
        from starrocks.coordinator.server import DaftCoordinatorServicer
        from starrocks.coordinator.proto import coordinator_pb2

        with mock.patch("starrocks.coordinator.server.DaftCoordinatorServicer._init_ray"):
            servicer = DaftCoordinatorServicer(ray_address=None)

        ctx = mock.MagicMock()
        resp = servicer.RegisterFunction(
            coordinator_pb2.RegisterFunctionRequest(
                function_name="bad",
                module_path="nonexistent_xyz",
                callable_name="func",
            ),
            ctx,
        )
        assert resp.success is False
        assert "ModuleNotFoundError" in resp.message or "ImportError" in resp.message

    @requires_daft
    def test_submit_plan_error_response(self):
        """SubmitDaftPlan with bad function should yield an error response."""
        from starrocks.coordinator.server import DaftCoordinatorServicer
        from starrocks.coordinator.proto import coordinator_pb2

        with mock.patch("starrocks.coordinator.server.DaftCoordinatorServicer._init_ray"):
            servicer = DaftCoordinatorServicer(ray_address=None)

        ctx = mock.MagicMock()
        request = coordinator_pb2.DaftPlanRequest(
            request_id="err-1",
            arrow_flight_endpoint="grpc://localhost:9408",
            source_sql="SELECT 1 as v",
            operations=[
                coordinator_pb2.DaftOperation(
                    map_batches=coordinator_pb2.MapBatchesOp(function_name="nonexistent"),
                ),
            ],
        )

        import pyarrow as pa
        table = pa.table({"v": [1]})
        with mock.patch.object(servicer._driver, "_fetch_source_data", return_value=table):
            responses = list(servicer.SubmitDaftPlan(request, ctx))

        assert len(responses) == 1
        assert responses[0].is_last is True
        assert "KeyError" in responses[0].error


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _decode_ipc_batches(batch_bytes_list: list[bytes]) -> "pa.Table":
    """Decode a list of Arrow IPC batch bytes into a single pyarrow Table."""
    import pyarrow as pa

    tables = []
    for data in batch_bytes_list:
        reader = pa.ipc.open_stream(data)
        tables.append(reader.read_all())
    return pa.concat_tables(tables)
