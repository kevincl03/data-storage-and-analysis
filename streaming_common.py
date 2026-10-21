"""Shared contracts and persistence helpers for the Hyperflix CDC pipeline."""

from __future__ import annotations

import json
import os
import tempfile
import base64
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from bson import BSON, ObjectId
from bson.timestamp import Timestamp


def serialize_value(value: Any) -> Any:
    """Convert BSON values into stable JSON/Parquet-compatible values."""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Timestamp):
        return {"timestamp": value.time, "increment": value.inc}
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(key): serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [serialize_value(item) for item in value]
    return value


def serialize_token(token: dict[str, Any] | None) -> str | None:
    if token is None:
        return None
    return base64.b64encode(BSON.encode(token)).decode("ascii")


def deserialize_token(value: str | None) -> dict[str, Any] | None:
    if not value:
        return None
    return BSON(base64.b64decode(value)).decode()


def event_identity(event: dict[str, Any]) -> str:
    token = event.get("resume_token")
    if token:
        return str(token)
    return "|".join(
        [
            str(event.get("operation_type", "")),
            str(event.get("namespace", "")),
            json.dumps(event.get("document_key", {}), sort_keys=True),
            str(event.get("cluster_time", "")),
        ]
    )


@dataclass(frozen=True)
class StreamingSettings:
    mongo_uri: str
    database_name: str
    watched_collections: tuple[str, ...]
    temporary_collection: str
    data_lake_root: Path
    checkpoint_path: Path
    batch_size: int
    flush_seconds: float
    payment_thresholds: dict[str, float]
    simultaneous_window_seconds: int
    marathon_minutes: int


def load_streaming_settings() -> StreamingSettings:
    from dotenv import load_dotenv

    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "").strip()
    if not mongo_uri:
        raise ValueError("Missing required environment variable: MONGO_URI")
    batch_size = int(os.getenv("STREAMING_BATCH_SIZE", "10"))
    flush_seconds = float(os.getenv("STREAMING_FLUSH_SECONDS", "10"))
    if batch_size <= 0 or flush_seconds <= 0:
        raise ValueError("STREAMING_BATCH_SIZE and STREAMING_FLUSH_SECONDS must be positive")
    root = Path(os.getenv("DATA_LAKE_ROOT", "data_lake"))
    checkpoint = Path(os.getenv("STREAMING_CHECKPOINT_PATH", str(root / "streaming" / "checkpoint.json")))
    collections = tuple(
        item.strip()
        for item in os.getenv("STREAMING_WATCH_COLLECTIONS", "reproducciones,suscripciones,pagos").split(",")
        if item.strip()
    )
    return StreamingSettings(
        mongo_uri=mongo_uri,
        database_name=os.getenv("STREAMING_DB_NAME", os.getenv("MONGO_DB_NAME", "hyperflix_db")).strip(),
        watched_collections=collections,
        temporary_collection=os.getenv("STREAMING_TEMP_COLLECTION", "streaming_cdc_demo").strip(),
        data_lake_root=root,
        checkpoint_path=checkpoint,
        batch_size=batch_size,
        flush_seconds=flush_seconds,
        payment_thresholds={
            "COP": float(os.getenv("STREAMING_HIGH_PAYMENT_COP", "500000")),
            "USD": float(os.getenv("STREAMING_HIGH_PAYMENT_USD", "15")),
        },
        simultaneous_window_seconds=int(os.getenv("STREAMING_SIMULTANEOUS_WINDOW_SECONDS", "300")),
        marathon_minutes=int(os.getenv("STREAMING_MARATHON_MINUTES", "240")),
    )


def change_event_to_record(change: dict[str, Any], ingestion_time: datetime | None = None) -> dict[str, Any]:
    namespace = change.get("ns", {})
    cluster_time = change.get("clusterTime")
    document_key = change.get("documentKey", {})
    update_description = change.get("updateDescription", {})
    record = {
        "operation_type": change.get("operationType"),
        "cluster_time": serialize_value(cluster_time),
        "wall_time": serialize_value(change.get("wallTime")),
        "namespace": f"{namespace.get('db', '')}.{namespace.get('coll', '')}",
        "document_key": serialize_value(document_key),
        "full_document": serialize_value(change.get("fullDocument")),
        "updated_fields": serialize_value(update_description.get("updatedFields", {})),
        "removed_fields": serialize_value(update_description.get("removedFields", [])),
        "resume_token": serialize_token(change.get("_id")),
        "ingestion_time": (ingestion_time or datetime.now(timezone.utc)).isoformat(),
    }
    record["event_id"] = event_identity(record)
    return record


def read_checkpoint(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_checkpoint(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"resume_token": record.get("resume_token"), "event_id": record.get("event_id"), "saved_at": datetime.now(timezone.utc).isoformat()}
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as temporary:
        json.dump(payload, temporary, ensure_ascii=False, indent=2)
        temporary_path = Path(temporary.name)
    temporary_path.replace(path)


def append_records(path: Path, records: Iterable[dict[str, Any]]) -> None:
    records = list(records)
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized_records = []
    for record in records:
        serialized = serialize_value(record)
        serialized_records.append(
            {
                key: json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
                if isinstance(value, (dict, list))
                else value
                for key, value in serialized.items()
            }
        )
    frame = pd.DataFrame(serialized_records)
    if path.exists():
        frame = pd.concat([pd.read_parquet(path, engine="pyarrow"), frame], ignore_index=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary_path, engine="pyarrow", compression="snappy", index=False)
    temporary_path.replace(path)


def append_partitioned_batch(root: Path, records: list[dict[str, Any]], batch_id: str) -> Path:
    event_date = datetime.now(timezone.utc).date().isoformat()
    path = root / "raw" / "cdc" / "events" / f"event_date={event_date}" / f"batch_{batch_id}.parquet"
    append_records(path, records)
    return path


def decode_bson_document(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, (bytes, bytearray)):
        return None
    return BSON(value).decode()