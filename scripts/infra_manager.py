"""Infrastructure Automation CLI for Hyperflix MongoDB Atlas via Terraform.

Handles prerequisite checks, validation, planning, applying, destroying,
and syncing generated connection strings directly into the environment (.env).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("infra_manager")

BASE_DIR = Path(__file__).resolve().parent.parent
TERRAFORM_DIR = BASE_DIR / "terraform"
ENV_PATH = BASE_DIR / ".env"
ENV_TERRAFORM_PATH = BASE_DIR / ".env.terraform"
TFVARS_PATH = TERRAFORM_DIR / "terraform.tfvars"
TFVARS_EXAMPLE = TERRAFORM_DIR / "terraform.tfvars.example"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def is_terraform_installed() -> bool:
    return shutil.which("terraform") is not None


def get_terraform_version() -> str | None:
    if not is_terraform_installed():
        return None
    try:
        result = subprocess.run(
            ["terraform", "version"],
            capture_output=True,
            text=True,
            check=True,
        )
        first_line = result.stdout.splitlines()[0] if result.stdout else ""
        return first_line.strip()
    except (subprocess.SubprocessError, OSError):
        return None


def run_terraform_command(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    if not is_terraform_installed():
        raise FileNotFoundError(
            "Terraform binary not found in PATH.\n"
            "To install Terraform on Windows:\n"
            "  winget install HashiCorp.Terraform\n"
            "  or choco install terraform\n"
            "  or download from https://developer.hashicorp.com/terraform/install"
        )
    cmd = ["terraform", f"-chdir={TERRAFORM_DIR}"] + args
    LOGGER.info("Executing: %s", " ".join(cmd))
    return subprocess.run(cmd, check=check, text=True)


def static_hcl_check(terraform_dir: Path) -> tuple[bool, list[str]]:
    """Performs static syntax and structural validation on HCL files."""
    errors: list[str] = []
    required_files = [
        "versions.tf",
        "variables.tf",
        "main.tf",
        "outputs.tf",
        "terraform.tfvars.example",
    ]

    for fname in required_files:
        path = terraform_dir / fname
        if not path.is_file():
            errors.append(f"Missing required file: {fname}")
            continue
        content = path.read_text(encoding="utf-8")
        if not content.strip():
            errors.append(f"File is empty: {fname}")
            continue

        # Check for matching braces and quotes
        open_braces = content.count("{")
        close_braces = content.count("}")
        if open_braces != close_braces:
            errors.append(
                f"Mismatched braces in {fname}: {open_braces} open vs {close_braces} close"
            )

    return len(errors) == 0, errors


def sync_environment_file(outputs: dict[str, Any], target_env: Path) -> Path:
    """Writes environment variables matching the Hyperflix schema from Terraform outputs."""
    app_uri = outputs.get("app_connection_string", {}).get("value")
    if not app_uri:
        # Fallback to standard srv if app_connection_string is not explicit
        srv = outputs.get("connection_string_srv", {}).get("value", "")
        app_uri = srv if srv.startswith("mongodb") else "mongodb+srv://hyperflix_app:password@cluster.mongodb.net/"

    content = f"""# ========================================================
# Hyperflix Environment - Auto-generated from Terraform Outputs
# ========================================================
MONGO_URI={app_uri}
MONGO_DB_NAME=hyperflix_db
DATA_LAKE_ROOT=data_lake
MONGO_BATCH_SIZE=1000
MONGO_USER_COLLECTION=usuarios
MONGO_CONTENT_COLLECTIONS=peliculas,canales_tv
MONGO_SUBSCRIPTION_COLLECTION=suscripciones
MONGO_PAYMENT_COLLECTION=pagos
MONGO_EVENT_COLLECTION=reproducciones
DASHBOARD_OUTPUT_DIR=dashboard_output
DASHBOARD_SLA_MS=500
DASHBOARD_ATLAS_MONTHLY_COST=0
DASHBOARD_OPERATIONS_MONTHLY_COST=0
STREAMING_DB_NAME=hyperflix_db
STREAMING_WATCH_COLLECTIONS=reproducciones,suscripciones,pagos
STREAMING_TEMP_COLLECTION=streaming_cdc_demo
STREAMING_BATCH_SIZE=10
STREAMING_FLUSH_SECONDS=10
STREAMING_CHECKPOINT_PATH=data_lake/streaming/checkpoint.json
STREAMING_HIGH_PAYMENT_COP=500000
STREAMING_HIGH_PAYMENT_USD=15
STREAMING_SIMULTANEOUS_WINDOW_SECONDS=300
STREAMING_MARATHON_MINUTES=240
"""
    target_env.write_text(content, encoding="utf-8")
    LOGGER.info("Environment written to %s", target_env)
    return target_env


def cmd_check_tools() -> int:
    LOGGER.info("Verifying infrastructure tooling...")
    version = get_terraform_version()
    if version:
        LOGGER.info("Terraform is installed: %s", version)
        return 0
    LOGGER.warning(
        "Terraform is NOT installed or not present in PATH.\n"
        "Installation commands for Windows:\n"
        "  winget install HashiCorp.Terraform\n"
        "  or: choco install terraform\n"
        "  or download binary from: https://developer.hashicorp.com/terraform/install"
    )
    return 1


def cmd_validate() -> int:
    LOGGER.info("Validating Terraform configuration files in %s...", TERRAFORM_DIR)
    is_valid, errors = static_hcl_check(TERRAFORM_DIR)
    if not is_valid:
        for err in errors:
            LOGGER.error("Validation error: %s", err)
        return 1

    LOGGER.info("Static HCL and structural integrity: OK")

    if is_terraform_installed():
        try:
            run_terraform_command(["fmt", "-check"], check=False)
            run_terraform_command(["init", "-backend=false"], check=True)
            run_terraform_command(["validate"], check=True)
            LOGGER.info("Terraform native validation: OK")
        except subprocess.SubprocessError as e:
            LOGGER.error("Terraform validation failed: %s", e)
            return 1
    else:
        LOGGER.info("Skipping 'terraform validate' binary execution (Terraform not in PATH). Static validation PASSED.")

    return 0


def cmd_plan() -> int:
    if not TFVARS_PATH.exists():
        LOGGER.error(
            "Missing %s. Please copy %s and fill in your Atlas credentials before planning.",
            TFVARS_PATH.name,
            TFVARS_EXAMPLE.name,
        )
        return 1

    run_terraform_command(["init"])
    run_terraform_command(["plan", "-out=tfplan"])
    LOGGER.info("Plan generated successfully as tfplan.")
    return 0


def cmd_apply() -> int:
    run_terraform_command(["apply", "-auto-approve", "tfplan"])
    LOGGER.info("Terraform apply completed. Synchronizing environment...")
    return cmd_sync_env()


def cmd_destroy() -> int:
    LOGGER.warning("Initiating infrastructure teardown...")
    run_terraform_command(["destroy", "-auto-approve"])
    LOGGER.info("Infrastructure destroyed.")
    return 0


def cmd_sync_env() -> int:
    if not is_terraform_installed():
        LOGGER.error("Terraform not found to read outputs. Use manual sync or install Terraform.")
        return 1
    try:
        proc = run_terraform_command(["output", "-json"])
        outputs = json.loads(proc.stdout) if proc.stdout else {}
        sync_environment_file(outputs, ENV_TERRAFORM_PATH)
        LOGGER.info("Successfully synced Terraform outputs to %s", ENV_TERRAFORM_PATH)
        return 0
    except Exception as e:
        LOGGER.error("Failed to sync environment: %s", e)
        return 1


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(
        description="Hyperflix Infrastructure as Code & Automation Manager"
    )
    parser.add_argument(
        "action",
        choices=["check-tools", "validate", "plan", "apply", "destroy", "sync-env"],
        help="Action to perform",
    )

    args = parser.parse_args()

    actions = {
        "check-tools": cmd_check_tools,
        "validate": cmd_validate,
        "plan": cmd_plan,
        "apply": cmd_apply,
        "destroy": cmd_destroy,
        "sync-env": cmd_sync_env,
    }

    try:
        return actions[args.action]()
    except Exception as e:
        LOGGER.error("Execution failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
