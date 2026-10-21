import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as parquet
from bson import ObjectId

import data_lake


class DataLakeLocalTests(unittest.TestCase):
    def test_documents_are_normalized_without_losing_nested_values(self):
        frame = data_lake.documents_to_frame(
            [
                {
                    "_id": ObjectId("507f1f77bcf86cd799439011"),
                    "preferences": {"language": "es"},
                    "tags": ["premium", "mobile"],
                }
            ]
        )

        self.assertEqual(frame.loc[0, "_id"], "507f1f77bcf86cd799439011")
        self.assertEqual(frame.loc[0, "preferences.language"], "es")
        self.assertEqual(frame.loc[0, "tags"], '["premium", "mobile"]')

    def test_processing_filters_completed_events_and_derives_dates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw_path = root / "reproducciones.parquet"
            processed_path = root / "reproducciones_processed.parquet"
            data_lake.write_parquet(
                pd.DataFrame(
                    [
                        {
                            "_id": "event-1",
                            "usuario_id": "user-1",
                            "contenido_id": "movie-1",
                            "fecha_inicio": datetime(2026, 1, 15, tzinfo=timezone.utc),
                            "estado": "Completada",
                            "duracion_vista_minutos": 42,
                        },
                        {
                            "_id": "event-2",
                            "usuario_id": "user-2",
                            "contenido_id": "movie-2",
                            "fecha_inicio": datetime(2026, 1, 16, tzinfo=timezone.utc),
                            "estado": "Abandonada",
                            "duracion_vista_minutos": 3,
                        },
                    ]
                ),
                raw_path,
            )

            frame, stats = data_lake.process_events(raw_path, processed_path)

            self.assertEqual(len(frame), 1)
            self.assertEqual(frame.loc[0, "anio"], 2026)
            self.assertEqual(frame.loc[0, "mes"], 1)
            self.assertEqual(stats["input_rows"], 2)
            self.assertEqual(stats["output_rows"], 1)

    def test_parquet_is_snappy_and_curated_outputs_are_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw"
            curated = root / "curated"
            raw.mkdir()
            curated.mkdir()
            data_lake.write_parquet(pd.DataFrame({"_id": ["user-1"], "nombre": ["Ada"]}), raw / "usuarios.parquet")
            data_lake.write_parquet(
                pd.DataFrame(
                    {
                        "_id": ["event-1"],
                        "usuario_id": ["user-1"],
                        "contenido_id": ["movie-1"],
                        "fecha_inicio": [pd.Timestamp("2026-01-15", tz="UTC")],
                        "anio": [2026],
                        "mes": [1],
                        "dia": [15],
                        "event_id": ["event-1"],
                        "user_id": ["user-1"],
                        "content_id": ["movie-1"],
                        "duracion_vista_minutos": [42],
                    }
                ),
                root / "processed.parquet",
            )

            metadata = parquet.read_metadata(raw / "usuarios.parquet")
            compression = metadata.row_group(0).column(0).compression
            self.assertEqual(compression, "SNAPPY")
            result = data_lake.build_curated(
                {"users": [raw / "usuarios.parquet"], "content": [], "subscriptions": []},
                data_lake.read_parquet(root / "processed.parquet"),
                curated,
            )

            self.assertIn("dim_usuario", result)
            self.assertIn("fact_reproducciones", result)
            self.assertTrue((curated / "consumo_analitico.parquet").exists())


if __name__ == "__main__":
    unittest.main()