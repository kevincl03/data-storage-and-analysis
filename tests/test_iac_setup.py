import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import iac_setup


class IacSetupTests(unittest.TestCase):
    def test_validar_version_python_accepts_current_version(self):
        # Current runtime must be valid
        self.assertTrue(iac_setup.validar_version_python((3, 8)))

    def test_validar_version_python_rejects_higher_version_requirement(self):
        # A requirement far into the future must be rejected
        self.assertFalse(iac_setup.validar_version_python((99, 0)))

    def test_resolver_rutas_entorno_structure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base_path = Path(temp_dir)
            paths = iac_setup.resolver_rutas_entorno(base_path)
            self.assertEqual(paths["env_dir"], base_path / "env")
            self.assertTrue(str(paths["python"]).endswith("python.exe") or str(paths["python"]).endswith("python"))
            self.assertTrue(str(paths["pip"]).endswith("pip.exe") or str(paths["pip"]).endswith("pip"))

    def test_provisionar_data_lake_creates_zones_idempotently(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lake_dir = Path(temp_dir) / "data_lake"
            zones = iac_setup.provisionar_data_lake(lake_dir)

            self.assertTrue((lake_dir / "raw").is_dir())
            self.assertTrue((lake_dir / "processed").is_dir())
            self.assertTrue((lake_dir / "curated").is_dir())
            self.assertTrue((lake_dir / "raw" / "cdc").is_dir())
            self.assertTrue((lake_dir / "curated" / "spark_jobs").is_dir())

            # Second execution must succeed without error (idempotence)
            zones_again = iac_setup.provisionar_data_lake(lake_dir)
            self.assertEqual(len(zones), len(zones_again))

    def test_health_check_aborts_cleanly_when_env_file_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_env = Path(temp_dir) / ".env.nonexistent"
            env_paths = iac_setup.resolver_rutas_entorno(Path(temp_dir))
            with self.assertRaises(SystemExit) as cm:
                iac_setup.ejecutar_health_check(env_paths, env_file=missing_env)
            self.assertEqual(cm.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
