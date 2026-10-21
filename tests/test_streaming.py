import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from bson import ObjectId
from bson.timestamp import Timestamp

from streaming_common import (
    StreamingSettings,
    append_records,
    change_event_to_record,
    deserialize_token,
    serialize_token,
    write_checkpoint,
)
from streaming_ingesta import StreamingProcessor


def settings_for(root: Path, batch_size: int = 2) -> StreamingSettings:
    return StreamingSettings(
        mongo_uri="mongodb://example",
        database_name="hyperflix_db",
        watched_collections=("reproducciones", "pagos", "suscripciones"),
        temporary_collection="streaming_cdc_demo",
        data_lake_root=root,
        checkpoint_path=root / "streaming" / "checkpoint.json",
        batch_size=batch_size,
        flush_seconds=60,
        payment_thresholds={"COP": 500000, "USD": 15},
        simultaneous_window_seconds=300,
        marathon_minutes=240,
    )


class StreamingTests(unittest.TestCase):
    def test_resume_token_round_trip_preserves_bson_types(self):
        token = {"_data": ObjectId("507f1f77bcf86cd799439011"), "cluster": Timestamp(3, 7)}
        restored = deserialize_token(serialize_token(token))
        self.assertEqual(restored, token)

    def test_change_event_extracts_insert_update_and_delete_fields(self):
        change = {
            "_id": {"_data": ObjectId("507f1f77bcf86cd799439011")},
            "operationType": "update",
            "clusterTime": Timestamp(2, 1),
            "wallTime": datetime.now(timezone.utc),
            "ns": {"db": "hyperflix_db", "coll": "reproducciones"},
            "documentKey": {"_id": ObjectId("507f1f77bcf86cd799439011")},
            "updateDescription": {"updatedFields": {"estado": "Pausada"}, "removedFields": ["minuto_actual"]},
            "fullDocument": {"estado": "Pausada"},
        }
        record = change_event_to_record(change)
        self.assertEqual(record["operation_type"], "update")
        self.assertEqual(record["updated_fields"], {"estado": "Pausada"})
        self.assertEqual(record["removed_fields"], ["minuto_actual"])
        self.assertEqual(record["namespace"], "hyperflix_db.reproducciones")

    def test_processor_deduplicates_and_flushes_checkpoint_after_persistence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            processor = StreamingProcessor(settings_for(root, batch_size=2))
            change = {
                "_id": {"_data": "token-1"},
                "operationType": "insert",
                "ns": {"db": "hyperflix_db", "coll": "reproducciones"},
                "documentKey": {"_id": "event-1"},
                "fullDocument": {"usuario_id": "user-1", "contenido_id": "movie-1", "calidad": "4K"},
            }
            self.assertIsNotNone(processor.process_change(change))
            self.assertIsNone(processor.process_change(change))
            change["_id"] = {"_data": "token-2"}
            self.assertIsNotNone(processor.process_change(change))
            checkpoint = processor.settings.checkpoint_path
            self.assertTrue(checkpoint.exists())
            self.assertEqual(len(list((root / "raw" / "cdc").rglob("*.parquet"))), 1)
            self.assertEqual(len(processor.alerts), 0)

    def test_high_payment_alert_is_generated(self):
        with tempfile.TemporaryDirectory() as directory:
            processor = StreamingProcessor(settings_for(Path(directory)))
            change = {
                "_id": {"_data": "payment-token"},
                "operationType": "insert",
                "ns": {"db": "hyperflix_db", "coll": "pagos"},
                "documentKey": {"_id": "payment-1"},
                "fullDocument": {"monto": 600000, "moneda": "COP"},
            }
            processor.process_change(change)
            self.assertEqual(processor.alerts[0]["alert_type"], "HIGH_PAYMENT")

    def test_simultaneous_streaming_alert_uses_distinct_devices(self):
        with tempfile.TemporaryDirectory() as directory:
            processor = StreamingProcessor(settings_for(Path(directory)))
            for index, device in enumerate(["Web", "Movil", "Smart TV"]):
                processor.process_change(
                    {
                        "_id": {"_data": f"stream-token-{index}"},
                        "operationType": "insert",
                        "ns": {"db": "hyperflix_db", "coll": "reproducciones"},
                        "documentKey": {"_id": f"event-{index}"},
                        "fullDocument": {"usuario_id": "user-1", "contenido_id": "movie-1", "dispositivo": device},
                    }
                )
            self.assertTrue(any(alert["alert_type"] == "SIMULTANEOUS_STREAMING" for alert in processor.alerts))

    def test_append_records_keeps_previous_batches(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.parquet"
            append_records(path, [{"event_id": "one"}])
            append_records(path, [{"event_id": "two"}])
            import pandas as pd

            self.assertEqual(list(pd.read_parquet(path)["event_id"]), ["one", "two"])


if __name__ == "__main__":
    unittest.main()