"""MongoDB Change Streams consumer for Hyperflix CDC."""

from __future__ import annotations

import logging
import time
from collections import Counter, deque
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from streaming_common import (
    StreamingSettings,
    append_partitioned_batch,
    append_records,
    change_event_to_record,
    deserialize_token,
    load_streaming_settings,
    read_checkpoint,
    write_checkpoint,
)


LOGGER = logging.getLogger("streaming_ingesta")


class StreamingProcessor:
    """Processes Change Stream records like a Kafka consumer plus Spark micro-batches."""

    def __init__(self, settings: StreamingSettings) -> None:
        self.settings = settings
        self.buffer: list[dict[str, Any]] = []
        self.seen_ids: set[str] = set()
        self.active_sessions: deque[tuple[str, datetime, str]] = deque()
        self.total_events = 0
        self.alerts: list[dict[str, Any]] = []
        self.metrics: Counter[str] = Counter()
        self.last_flush = time.monotonic()

    def process_change(self, change: dict[str, Any]) -> dict[str, Any] | None:
        record = change_event_to_record(change)
        if record["event_id"] in self.seen_ids:
            return None
        self.seen_ids.add(record["event_id"])
        self.total_events += 1
        self.buffer.append(record)
        self.metrics[record["operation_type"]] += 1
        self._evaluate_alerts(record)
        LOGGER.info(self._format_change(record))
        if self.should_flush():
            self.flush()
        return record

    def should_flush(self) -> bool:
        return len(self.buffer) >= self.settings.batch_size or time.monotonic() - self.last_flush >= self.settings.flush_seconds

    def flush(self) -> Path | None:
        if not self.buffer:
            return None
        batch = self.buffer[:]
        batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        raw_path = append_partitioned_batch(self.settings.data_lake_root, batch, batch_id)
        processed_path = self.settings.data_lake_root / "processed" / "streaming" / "events.parquet"
        alert_path = self.settings.data_lake_root / "curated" / "streaming" / "alerts.parquet"
        metrics_path = self.settings.data_lake_root / "curated" / "streaming" / "metrics.parquet"
        append_records(processed_path, batch)
        append_records(alert_path, self.alerts)
        append_records(metrics_path, [{"batch_id": batch_id, "event_count": len(batch), **self.metrics}])
        write_checkpoint(self.settings.checkpoint_path, batch[-1])
        self.buffer.clear()
        self.alerts.clear()
        self.last_flush = time.monotonic()
        LOGGER.info("[MICRO-BATCH] Procesados %s eventos. Persistido %s", len(batch), raw_path)
        return raw_path

    def _evaluate_alerts(self, record: dict[str, Any]) -> None:
        document = record.get("full_document") or {}
        namespace = record.get("namespace", "")
        if namespace.endswith(".pagos"):
            amount = float(document.get("monto", 0) or 0)
            currency = str(document.get("moneda", "COP")).upper()
            threshold = self.settings.payment_thresholds.get(currency)
            if threshold is not None and amount > threshold:
                self.alerts.append({"alert_type": "HIGH_PAYMENT", "severity": "HIGH", "message": f"High payment detected: {amount} {currency}", "event_id": record["event_id"], "created_at": datetime.now(timezone.utc).isoformat()})
                LOGGER.warning("[ALERTA PAGO ALTO] %s %s", amount, currency)
        if namespace.endswith(".reproducciones"):
            quality = document.get("calidad")
            if quality == "4K":
                self.alerts.append({"alert_type": "PREMIUM_QUALITY", "severity": "INFO", "message": "4K reproduction detected", "event_id": record["event_id"], "created_at": datetime.now(timezone.utc).isoformat()})
            user_id = str(document.get("usuario_id", ""))
            device = str(document.get("dispositivo", ""))
            now = datetime.now(timezone.utc)
            window_start = now.timestamp() - self.settings.simultaneous_window_seconds
            self.active_sessions = deque(
                (active_user, started_at, active_device)
                for active_user, started_at, active_device in self.active_sessions
                if started_at.timestamp() >= window_start
            )
            self.active_sessions.append((user_id, now, device))
            devices = {active_device for active_user, _, active_device in self.active_sessions if active_user == user_id and active_device}
            if len(devices) >= 3:
                self.alerts.append({"alert_type": "SIMULTANEOUS_STREAMING", "severity": "HIGH", "message": f"User {user_id} active on {len(devices)} devices", "event_id": record["event_id"], "created_at": now.isoformat()})
                LOGGER.warning("[ALERTA SESIONES SIMULTANEAS] user=%s devices=%s", user_id, len(devices))
            duration = float(document.get("duracion_vista_minutos", 0) or 0)
            if duration >= self.settings.marathon_minutes:
                self.alerts.append({"alert_type": "MARATHON", "severity": "MEDIUM", "message": f"Long reproduction: {duration} minutes", "event_id": record["event_id"], "created_at": datetime.now(timezone.utc).isoformat()})

    @staticmethod
    def _format_change(record: dict[str, Any]) -> str:
        operation = record["operation_type"].upper()
        if operation == "INSERT":
            document = record.get("full_document") or {}
            return f"[{operation}] {record['namespace']} user={document.get('usuario_id')} content={document.get('contenido_id')}"
        if operation == "UPDATE":
            return f"[{operation}] {record['namespace']} key={record['document_key']} fields={record['updated_fields']}"
        return f"[{operation}] {record['namespace']} key={record['document_key']}"


def simular_streaming_kafka() -> None:
    settings = load_streaming_settings()
    processor = StreamingProcessor(settings)
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5_000, connectTimeoutMS=5_000, appname="hyperflix-cdc-consumer")
    try:
        database = client[settings.database_name]
        checkpoint = read_checkpoint(settings.checkpoint_path)
        watched_collections = tuple(dict.fromkeys((*settings.watched_collections, settings.temporary_collection)))
        pipeline = [{"$match": {"ns.coll": {"$in": list(watched_collections)}}}]
        watch_options = {"full_document": "updateLookup", "max_await_time_ms": 1_000}
        if checkpoint and checkpoint.get("resume_token"):
            watch_options["resume_after"] = deserialize_token(checkpoint["resume_token"])
        LOGGER.info("INICIANDO PIPELINE CDC sobre %s", ", ".join(watched_collections))
        with database.watch(pipeline, **watch_options) as stream:
            for change in stream:
                processor.process_change(change)
    except KeyboardInterrupt:
        LOGGER.info("Cerrando Change Stream y desconectando de MongoDB Atlas...")
    except PyMongoError:
        LOGGER.exception("Change Stream failed; inspect the checkpoint and Atlas replica set configuration")
        raise
    finally:
        processor.flush()
        client.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    simular_streaming_kafka()