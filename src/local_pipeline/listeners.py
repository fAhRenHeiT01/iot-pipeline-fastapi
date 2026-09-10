"""StreamingQueryListener providing local pipeline micro-batch observability."""

from typing import Any

from pyspark.sql.streaming import StreamingQueryListener

from common.logger import get_logger

logger = get_logger("local-streaming-listener")


class LocalPipelineStreamingListener(StreamingQueryListener):
    """Logs progress, throughput, and error events for local PySpark streaming queries."""

    def __init__(self, db: Any = None):
        super().__init__()
        self.db = db

    def onQueryStarted(self, event: Any) -> None:
        logger.info(
            "Streaming query started: name=%s, id=%s, runId=%s",
            event.name,
            event.id,
            event.runId,
        )

    def onQueryProgress(self, event: Any) -> None:
        progress = event.progress
        num_input_rows = progress.numInputRows
        processed_rows_per_sec = progress.processedRowsPerSecond
        input_rows_per_sec = progress.inputRowsPerSecond
        batch_id = progress.batchId

        if self.db is not None:
            try:
                self.db.record_stream_progress(
                    query_name=str(progress.name or "unknown_stream"),
                    batch_id=int(batch_id),
                    num_input_rows=int(num_input_rows),
                    input_rows_per_sec=float(input_rows_per_sec),
                    processed_rows_per_sec=float(processed_rows_per_sec),
                )
            except Exception as exc:
                logger.warning("Failed to record streaming progress to database: %s", exc)

        if num_input_rows > 0:
            logger.info(
                "[%s | Batch %s] Rows: %s | In rate: %.1f r/s | Proc rate: %.1f r/s",
                progress.name,
                batch_id,
                num_input_rows,
                input_rows_per_sec,
                processed_rows_per_sec,
            )

    def onQueryTerminated(self, event: Any) -> None:
        if event.exception:
            logger.error(
                "Streaming query terminated with error: id=%s, runId=%s, exception=%s",
                event.id,
                event.runId,
                event.exception,
            )
        else:
            logger.info(
                "Streaming query terminated cleanly: id=%s, runId=%s",
                event.id,
                event.runId,
            )
