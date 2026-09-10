"""Typer CLI entrypoint for running the telemetry generator."""

from pathlib import Path
import time

import typer

from common.config import get_settings
from common.logger import get_logger
from producer.generator import TelemetryGenerator

app = typer.Typer(help="IoT Fleet Telemetry Producer CLI")
logger = get_logger("producer-cli")


@app.command()
def start(
    rate: int = typer.Option(5, help="Events per second to generate"),
    duration: int = typer.Option(30, help="Duration to run in seconds (0 for indefinite)"),
    vehicles: int = typer.Option(20, help="Number of distinct vehicles in fleet"),
    dry_run: bool = typer.Option(False, help="Print events to stdout instead of sending to Kafka"),
    topic: str | None = typer.Option(None, help="Kafka topic override"),
) -> None:
    """Start streaming generated telemetry events."""
    settings = get_settings()
    target_topic = topic or settings.KAFKA_TOPIC_RAW
    generator = TelemetryGenerator(num_vehicles=vehicles)

    logger.info(
        f"Starting producer: rate={rate}/s, duration={duration}s, "
        f"dry_run={dry_run}, topic={target_topic}"
    )

    producer = None
    if not dry_run:
        try:
            from confluent_kafka import Producer

            kafka_conf = {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "security.protocol": settings.KAFKA_SECURITY_PROTOCOL,
                "sasl.mechanism": settings.KAFKA_SASL_MECHANISM,
            }
            if settings.KAFKA_USER and settings.KAFKA_PASSWORD:
                kafka_conf["sasl.username"] = settings.KAFKA_USER
                kafka_conf["sasl.password"] = settings.KAFKA_PASSWORD

            if settings.KAFKA_CA_CERT_PATH:
                cert_path = Path(settings.KAFKA_CA_CERT_PATH)
                kafka_conf["ssl.ca.location"] = (
                    str(cert_path.resolve()) if cert_path.exists() else settings.KAFKA_CA_CERT_PATH
                )

            producer = Producer(kafka_conf)
        except Exception as exc:
            logger.warning(f"Could not connect to Kafka ({exc}). Falling back to dry_run mode.")
            dry_run = True

    start_time = time.time()
    events_sent = 0
    sleep_interval = 1.0 / max(rate, 1)

    try:
        while True:
            if duration > 0 and (time.time() - start_time) >= duration:
                break

            payload, anomaly_type = generator.generate_event()

            if dry_run or producer is None:
                logger.info(f"[{anomaly_type.value}] {payload}")
            else:
                producer.produce(
                    target_topic,
                    value=payload.encode("utf-8"),
                )
                producer.poll(0)

            events_sent += 1
            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    finally:
        if producer is not None:
            producer.flush(timeout=5.0)
        logger.info(f"Finished. Total events sent: {events_sent}")


@app.command()
def sample(
    count: int = typer.Option(5, help="Number of sample events to generate"),
    vehicles: int = typer.Option(10, help="Number of distinct vehicles in fleet"),
) -> None:
    """Generate and display sample telemetry events to stdout."""
    generator = TelemetryGenerator(num_vehicles=vehicles)
    for _ in range(count):
        payload, anomaly = generator.generate_event()
        logger.info(f"[{anomaly.value}] {payload}")


if __name__ == "__main__":
    app()
