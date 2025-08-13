"""A Crossplane composition function."""

import grpc
from crossplane.function import logging
from crossplane.function.proto.v1 import run_function_pb2 as fnv1
from crossplane.function.proto.v1 import run_function_pb2_grpc as grpcv1


class FunctionRunner(grpcv1.FunctionRunnerService):
    """A FunctionRunner handles gRPC RunFunctionRequests."""

    def __init__(self):
        """Create a new FunctionRunner."""
        self.log = logging.get_logger()

    async def RunFunction(
        self, req: fnv1.RunFunctionRequest, _: grpc.aio.ServicerContext
    ) -> fnv1.RunFunctionResponse:
        """Run the function."""
        # Some proto fields may be unset during render; access defensively.
        try:  # pragma: no cover - simple defensive access
            tag = req.meta.tag
        except Exception:  # noqa: BLE001
            tag = ""
        log = self.log.bind(tag=tag)
        log.info("Running function")

        # Build a minimal empty response to verify wiring works without
        # depending on any optional request fields being present.
        log.info("Scaffold run complete")
        return fnv1.RunFunctionResponse()
