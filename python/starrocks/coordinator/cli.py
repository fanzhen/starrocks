"""CLI entry point for the Daft Coordinator gRPC server."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from concurrent import futures

import grpc

from starrocks.coordinator.proto import coordinator_pb2_grpc
from starrocks.coordinator.server import DaftCoordinatorServicer

logger = logging.getLogger("starrocks.coordinator")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="StarRocks Daft Coordinator — gRPC sidecar for Daft-on-Ray execution",
    )
    parser.add_argument("--port", type=int, default=50051,
                        help="gRPC listen port (default: 50051)")
    parser.add_argument("--ray-address", type=str, default=None,
                        help="Ray cluster address (e.g. ray://head:10001)")
    parser.add_argument("--max-workers", type=int, default=10,
                        help="Max gRPC thread pool workers (default: 10)")
    parser.add_argument("--functions-dir", type=str, default=None,
                        help="Directory for persisting function registrations "
                             "(default: ~/.starrocks/coordinator)")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging level (default: INFO)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.max_workers))
    servicer = DaftCoordinatorServicer(ray_address=args.ray_address,
                                       functions_dir=args.functions_dir)
    coordinator_pb2_grpc.add_DaftCoordinatorServicer_to_server(servicer, server)

    listen_addr = f"[::]:{args.port}"
    server.add_insecure_port(listen_addr)
    server.start()
    logger.info("Daft Coordinator serving on %s", listen_addr)

    # Graceful shutdown on SIGINT/SIGTERM.
    shutdown_event = server.wait_for_termination

    def _handle_signal(signum, frame):
        logger.info("Received signal %d, shutting down...", signum)
        servicer.shutdown()
        server.stop(grace=5)
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    shutdown_event()


if __name__ == "__main__":
    main()
