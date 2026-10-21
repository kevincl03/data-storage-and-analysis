import tempfile
import unittest
from pathlib import Path

import matplotlib.pyplot as plt

from scripts import dashboard_monitoreo


class DashboardOlapTests(unittest.TestCase):
    def test_dashboard_generates_png_when_costs_are_not_configured(self):
        timings = [{"query": index, "name": f"Query {index}", "latency_ms": 100.0, "rows": 1} for index in range(1, 11)]
        results = [{"data": {"consumption": [{"_id": {"anio": 2026, "mes": 1}, "reproducciones": 2, "horas_totales": 1.5}]}}]
        storage = {
            "collections": {
                "dim_tiempo": {"count": 1, "storageSize": 100},
                "dim_usuario": {"count": 1, "storageSize": 100},
                "dim_contenido": {"count": 1, "storageSize": 100},
                "fact_visualizaciones": {"count": 1, "storageSize": 100},
            },
            "total_docs": 4,
            "total_bytes": 400,
            "total_gb": 0.000001,
        }
        with tempfile.TemporaryDirectory() as directory:
            output = dashboard_monitoreo.generate_dashboard(results, timings, storage, Path(directory))
            self.assertTrue(output.exists())
            self.assertGreater(output.stat().st_size, 0)
        plt.close("all")

    def test_query_catalog_contains_ten_olap_queries(self):
        self.assertEqual(len(dashboard_monitoreo.QUERIES), 10)


if __name__ == "__main__":
    unittest.main()