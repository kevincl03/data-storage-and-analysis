import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from scripts import spark_jobs


class SparkJobsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_jobs.crear_spark_session()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self._write_fixture_data()
        self.outputs = {}

    def tearDown(self):
        self.directory.cleanup()

    def _write_fixture_data(self):
        curated = self.root / "curated"
        raw = self.root / "raw"
        curated.mkdir()
        raw.mkdir()
        dates = [
            datetime(2026, 1, 15, tzinfo=timezone.utc),
            datetime(2026, 1, 16, tzinfo=timezone.utc),
            datetime(2026, 2, 2, tzinfo=timezone.utc),
        ]
        pd.DataFrame(
            {
                "event_id": ["event-1", "event-2", "event-3"],
                "user_id": ["user-1", "user-1", "user-2"],
                "content_id": ["movie-1", "movie-1", "movie-2"],
                "fecha_inicio": dates,
                "duration_minutes": [3000, 2400, 30],
            }
        ).to_parquet(curated / "fact_reproducciones.parquet", index=False)
        pd.DataFrame(
            {
                "content_id": ["movie-1", "movie-2"],
                "content_type": ["peliculas", "peliculas"],
                "titulo": ["Movie One", "Movie Two"],
                "generos": ['["Drama", "Sci-Fi"]', '["Drama"]'],
                "categoria": [None, None],
            }
        ).to_parquet(curated / "dim_contenido.parquet", index=False)
        pd.DataFrame(
            {"user_id": ["user-1", "user-2"], "pais": ["Colombia", "Mexico"], "ciudad": ["Bogota", "Monterrey"]}
        ).to_parquet(curated / "dim_usuario.parquet", index=False)
        pd.DataFrame(
            {
                "usuario_id": ["user-1", "user-2"],
                "suscripcion_id": ["sub-1", "sub-2"],
                "fecha_pago": [dates[0], dates[1]],
                "monto": [29900, 14900],
                "moneda": ["COP", "COP"],
                "estado": ["Completado", "Pendiente"],
            }
        ).to_parquet(raw / "pagos.parquet", index=False)

    def _capture_write(self, frame, path):
        self.outputs[str(path)] = frame

    def test_job1_explodes_json_genres_and_aggregates_content(self):
        with patch.object(spark_jobs, "_write", side_effect=self._capture_write):
            result = spark_jobs.ejecutar_job1_popularidad_contenido(self.spark, self.root)
        rows = {(row.content_id, row.genero): row.total_reproducciones for row in result.collect()}
        self.assertEqual(rows[("movie-1", "Drama")], 2)
        self.assertEqual(rows[("movie-1", "Sci-Fi")], 2)
        self.assertTrue(any("metricas_contenido" in path for path in self.outputs))

    def test_job2_assigns_engagement_segments(self):
        with patch.object(spark_jobs, "_write", side_effect=self._capture_write):
            result = spark_jobs.ejecutar_job2_segmentacion_usuarios(self.spark, self.root)
        segments = {row.user_id: row.segmento for row in result.collect()}
        self.assertEqual(segments["user-1"], "VIP Binge Watcher")
        self.assertEqual(segments["user-2"], "Usuario Ocasional")

    def test_job3_builds_consumption_trends_and_separate_payment_summary(self):
        with patch.object(spark_jobs, "_write", side_effect=self._capture_write):
            monthly, daily, payments = spark_jobs.ejecutar_job3_tendencias_temporales(self.spark, self.root)
        monthly_rows = {(row.anio, row.mes): row.total_reproducciones for row in monthly.collect()}
        self.assertEqual(monthly_rows[(2026, 1)], 2)
        self.assertEqual(monthly_rows[(2026, 2)], 1)
        self.assertEqual(daily.count(), 3)
        self.assertEqual(payments.count(), 1)
        self.assertEqual(payments.first().monto_total, 29900.0)
        self.assertTrue(any("pagos_resumen" in path for path in self.outputs))


if __name__ == "__main__":
    unittest.main()