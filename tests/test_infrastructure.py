"""Unit tests for Hyperflix Infrastructure as Code (Terraform & Automation)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts import infra_manager


class InfrastructureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.terraform_dir = Path(__file__).resolve().parent.parent / "terraform"

    def test_all_terraform_files_exist_and_balanced(self) -> None:
        is_valid, errors = infra_manager.static_hcl_check(self.terraform_dir)
        self.assertTrue(is_valid, f"Static check failed with errors: {errors}")

    def test_variables_contract_and_free_tier_defaults(self) -> None:
        var_file = self.terraform_dir / "variables.tf"
        content = var_file.read_text(encoding="utf-8")

        self.assertIn('variable "atlas_org_id"', content)
        self.assertIn('variable "atlas_public_key"', content)
        self.assertIn('variable "atlas_private_key"', content)
        self.assertIn('variable "cluster_tier"', content)
        self.assertIn('default     = "M0"', content)
        self.assertIn('default     = "AWS"', content)
        self.assertIn('default     = "US_EAST_1"', content)

    def test_main_tf_declares_core_atlas_resources(self) -> None:
        main_file = self.terraform_dir / "main.tf"
        content = main_file.read_text(encoding="utf-8")

        self.assertIn('resource "mongodbatlas_project"', content)
        self.assertIn('resource "mongodbatlas_advanced_cluster"', content)
        self.assertIn('resource "mongodbatlas_project_ip_access_list"', content)
        self.assertIn('resource "mongodbatlas_database_user"', content)
        self.assertIn('resource "local_file" "env_file"', content)
        self.assertIn('provider_name         = local.provider_name', content)

    def test_outputs_contract(self) -> None:
        outputs_file = self.terraform_dir / "outputs.tf"
        content = outputs_file.read_text(encoding="utf-8")

        self.assertIn('output "project_id"', content)
        self.assertIn('output "cluster_id"', content)
        self.assertIn('output "connection_string_srv"', content)
        self.assertIn('output "app_connection_string"', content)
        self.assertIn('output "reader_connection_string"', content)
        self.assertIn('output "env_file_path"', content)

    def test_sync_environment_file_creates_compatible_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            target_env = Path(tmp_dir) / ".env.test"
            mock_outputs = {
                "app_connection_string": {
                    "value": "mongodb+srv://hyperflix_app:secret123@hyperflix-cluster.mongodb.net/?retryWrites=true&w=majority"
                }
            }
            infra_manager.sync_environment_file(mock_outputs, target_env)

            self.assertTrue(target_env.exists())
            content = target_env.read_text(encoding="utf-8")
            self.assertIn("MONGO_URI=mongodb+srv://hyperflix_app:secret123@", content)
            self.assertIn("MONGO_DB_NAME=hyperflix_db", content)
            self.assertIn("DATA_LAKE_ROOT=data_lake", content)
            self.assertIn("STREAMING_DB_NAME=hyperflix_db", content)


if __name__ == "__main__":
    unittest.main()
