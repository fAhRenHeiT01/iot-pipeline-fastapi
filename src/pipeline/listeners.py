"""StreamingQueryListener implementation for monitoring SLA lag and micro-batch metrics."""

from typing import Any

from common.logger import get_logger

logger = get_logger("streaming-listener")

try:
    from pyspark.sql.streaming import StreamingQueryListener
except ImportError:  # pragma: no cover
    class StreamingQueryListener:  # type: ignore
        """Mock StreamingQueryListener if PySpark not installed."""
        pass


class SlaMetricsListener(StreamingQueryListener):
    """Custom listener tracking trigger execution duration and input rates."""

    def onQueryStarted(self, event: Any) -> None:
        logger.info(f"Stream query started: id={event.id}, name={event.name}")

    def onQueryProgress(self, event: Any) -> None:
        progress = event.progress
        batch_id = progress.batchId
        num_input_rows = progress.numInputRows
        input_rows_per_second = progress.inputRowsPerSecond
        processed_rows_per_second = progress.processedRowsPerSecond

        duration_ms = progress.durationMs.get("triggerExecution", 0) if progress.durationMs else 0

        logger.info(
            f"Query progress [batch={batch_id}]: "
            f"input_rows={num_input_rows}, "
            f"input_rate={input_rows_per_second:.2f}/s, "
            f"process_rate={processed_rows_per_second:.2f}/s, "
            f"trigger_time={duration_ms}ms"
        )

    def onQueryTerminated(self, event: Any) -> None:
        if event.exception:
            logger.error(f"Stream query terminated with error: {event.exception}")
        else:
            logger.info("Stream query terminated gracefully.")


# Alias matching entrypoint snippet
FleetPipelineListener = SlaMetricsListener


