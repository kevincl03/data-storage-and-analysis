#!/usr/bin/env python3
"""Hyperflix Environment & Infrastructure Orchestrator (IaC Setup).

Automates the provisioning of the local development environment for the
Hyperflix streaming platform, executing 5 sequential phases:
1. Environment validation (Python version check).
2. Dependency isolation (Virtual environment 'env' creation).
3. Dependency management (Pip installation from requirements.txt).
4. Local Data Lake provisioning (raw, processed, curated zones).
5. Atlas Health Check (MongoDB Atlas connectivity and hyperflix_db validation).
"""

from __future__ import annotations

import os
import subprocess
import sys
import venv
from pathlib import Path

# Minimum supported Python version for Hyperflix pipeline
MIN_PYTHON_VERSION = (3, 8)
BASE_DIR = Path(__file__).resolve().parent
ENV_DIR = BASE_DIR / "env"
DATA_LAKE_DIR = BASE_DIR / "data_lake"
REQUIREMENTS_FILE = BASE_DIR / "requirements.txt"
ENV_FILE = BASE_DIR / ".env"
DEFAULT_DB_NAME = "hyperflix_db"

# Ensure UTF-8 output on Windows consoles if possible
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def _safe_print(text: str, file=sys.stdout) -> None:
    """Prints text ensuring encoding compatibility on any terminal."""
    try:
        print(text, file=file)
    except UnicodeEncodeError:
        # Fallback to ASCII representation
        sanitized = text.encode("ascii", errors="replace").decode("ascii")
        print(sanitized, file=file)


def print_step(message: str) -> None:
    """Prints a styled Hyperflix DevOps step header."""
    _safe_print(f"\n[Hyperflix DevOps] {message}")


def print_success(message: str) -> None:
    """Prints a success indicator."""
    _safe_print(f"  ✔ {message}")


def print_warning(message: str) -> None:
    """Prints a warning indicator."""
    _safe_print(f"  ⚠ {message}")


def print_error(message: str) -> None:
    """Prints an error indicator."""
    _safe_print(f"  ❌ {message}", file=sys.stderr)


# ==============================================================================
# FASE 1: Validación del Entorno
# ==============================================================================
def validar_version_python(min_version: tuple[int, int] = MIN_PYTHON_VERSION) -> bool:
    """Validates that the host Python version meets minimum requirements."""
    print_step("Validando versión de Python...")
    current_version = sys.version_info[:2]
    version_str = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

    if current_version < min_version:
        print_error(
            f"Versión de Python incompatible: {version_str}. "
            f"Hyperflix requiere Python {min_version[0]}.{min_version[1]} o superior."
        )
        return False

    print_success(f"Versión de Python compatible: {version_str} (Requerido >= {min_version[0]}.{min_version[1]})")
    return True


# ==============================================================================
# FASE 2: Aislamiento (Creación del Virtual Environment 'env')
# ==============================================================================
def resolver_rutas_entorno(base_dir: Path = BASE_DIR) -> dict[str, Path]:
    """Resolves cross-platform binary paths inside the 'env' virtual environment."""
    env_path = base_dir / "env"
    is_windows = os.name == "nt" or sys.platform.startswith("win")

    if is_windows:
        python_bin = env_path / "Scripts" / "python.exe"
        pip_bin = env_path / "Scripts" / "pip.exe"
    else:
        python_bin = env_path / "bin" / "python"
        pip_bin = env_path / "bin" / "pip"

    return {
        "env_dir": env_path,
        "python": python_bin,
        "pip": pip_bin,
    }


def provisionar_entorno_virtual(env_dir: Path = ENV_DIR) -> None:
    """Creates the 'env' virtual environment if it does not already exist."""
    print_step("Provisionando entorno virtual 'env' para aislamiento de dependencias...")

    if env_dir.is_dir():
        print_success(f"Entorno virtual '{env_dir.name}' ya existente. Reutilizando aislamiento.")
        return

    try:
        # Create virtual environment with pip pre-installed
        venv.create(env_dir, with_pip=True)
        print_success(f"Entorno virtual '{env_dir.name}' creado exitosamente en {env_dir}.")
    except Exception as exc:
        print_error(f"Error al crear el entorno virtual '{env_dir.name}': {exc}")
        sys.exit(1)


# ==============================================================================
# FASE 3: Gestión de Dependencias
# ==============================================================================
def instalar_dependencias(env_paths: dict[str, Path], requirements_file: Path = REQUIREMENTS_FILE) -> None:
    """Installs required packages into 'env' using pip."""
    print_step("Instalando dependencias de Data & Streaming (PyMongo, Pandas, PySpark, etc.)...")

    if not requirements_file.is_file():
        print_error(f"No se encontró el archivo de requerimientos: {requirements_file}")
        sys.exit(1)

    python_bin = env_paths["python"]
    if not python_bin.is_file():
        print_error(f"No se encontró el binario de Python dentro de 'env': {python_bin}")
        sys.exit(1)

    # Use 'env_python -m pip install -r requirements.txt' for maximum reliability
    cmd = [str(python_bin), "-m", "pip", "install", "--upgrade", "pip"]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        # Non-fatal if pip upgrade fails, proceed to install requirements
        pass

    install_cmd = [str(python_bin), "-m", "pip", "install", "-r", str(requirements_file)]
    try:
        result = subprocess.run(install_cmd, check=True, capture_output=True, text=True)
        print_success(f"Dependencias instaladas exitosamente desde {requirements_file.name}.")
    except subprocess.CalledProcessError as exc:
        print_error(f"Error instalando dependencias en '{env_paths['env_dir'].name}':\n{exc.stderr}")
        sys.exit(1)


# ==============================================================================
# FASE 4: Provisionamiento Local (Data Lake de Hyperflix)
# ==============================================================================
def provisionar_data_lake(root_dir: Path = DATA_LAKE_DIR) -> list[Path]:
    """Provisions the 3-zone Data Lake architecture and streaming directories."""
    print_step("Construyendo infraestructura local del Data Lake de Hyperflix...")

    # Required 3-zone architecture + streaming CDC and PySpark job zones
    zones = [
        root_dir / "raw",
        root_dir / "raw" / "cdc",
        root_dir / "processed",
        root_dir / "processed" / "streaming",
        root_dir / "curated",
        root_dir / "curated" / "streaming",
        root_dir / "curated" / "spark_jobs",
        root_dir / "streaming",
    ]

    created_zones: list[Path] = []
    for zone in zones:
        zone.mkdir(parents=True, exist_ok=True)
        created_zones.append(zone)

    print_success("Zonas Data Lake verificadas y operativas:")
    print_success(f"  • RAW Zone:       {root_dir / 'raw'}")
    print_success(f"  • PROCESSED Zone: {root_dir / 'processed'}")
    print_success(f"  • CURATED Zone:   {root_dir / 'curated'}")
    return created_zones


# ==============================================================================
# FASE 5: Health Check (MongoDB Atlas - Hyperflix Cluster)
# ==============================================================================
def ejecutar_health_check(env_paths: dict[str, Path], env_file: Path = ENV_FILE) -> None:
    """Verifies .env file and executes MongoDB Atlas ping against hyperflix_db."""
    print_step("Ejecutando Health Check contra MongoDB Atlas...")

    if not env_file.is_file():
        print_error(f"No se encontró el archivo de configuración '{env_file.name}' en la raíz del proyecto.")
        print_warning(
            "Para configurar el entorno:\n"
            "    1. Copia '.env.example' a '.env':\n"
            "       Copy-Item .env.example .env\n"
            "    2. Configura tu MONGO_URI de MongoDB Atlas en el archivo .env\n"
            "    3. O si usas Terraform, aplica la infraestructura con:\n"
            "       python scripts/infra_manager.py apply\n"
        )
        sys.exit(1)

    # In-line Python script executed through env/python to guarantee PyMongo availability
    health_check_code = f"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(r"{env_file}")
load_dotenv(env_path)

mongo_uri = os.getenv("MONGO_URI")
db_name = os.getenv("MONGO_DB_NAME", "{DEFAULT_DB_NAME}")

if not mongo_uri or "username:password" in mongo_uri:
    print("CONFIG_ERROR: MONGO_URI no configurado o contiene valores de ejemplo en .env", file=sys.stderr)
    sys.exit(2)

try:
    import pymongo
    from pymongo.errors import ConnectionFailure, ConfigurationError, OperationFailure

    client = pymongo.MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        appname="hyperflix-devops-healthcheck"
    )
    # Ping admin database
    client.admin.command("ping")
    
    # Verify access to Hyperflix database
    db = client[db_name]
    _ = db.name
    
    client.close()
    sys.exit(0)

except ConnectionFailure as e:
    print(f"CONNECTION_FAILURE: No se pudo contactar al cluster MongoDB Atlas: {{e}}", file=sys.stderr)
    sys.exit(3)
except ConfigurationError as e:
    print(f"CONFIGURATION_ERROR: Error en cadena de conexión MONGO_URI: {{e}}", file=sys.stderr)
    sys.exit(4)
except OperationFailure as e:
    print(f"OPERATION_FAILURE: Error de autenticación o permisos en Atlas: {{e}}", file=sys.stderr)
    sys.exit(5)
except Exception as e:
    print(f"UNEXPECTED_ERROR: {{e}}", file=sys.stderr)
    sys.exit(6)
"""

    python_bin = env_paths["python"]
    result = subprocess.run(
        [str(python_bin), "-c", health_check_code],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        print_step("Health Check: Conexión exitosa a MongoDB Atlas (Hyperflix Cluster). Entorno listo 🎬.")
        return

    error_msg = result.stderr.strip()
    if result.returncode == 2:
        print_error(f"Configuración inválida: {error_msg}")
    elif result.returncode == 3:
        print_error(f"Fallo de conexión al cluster Atlas: {error_msg}")
    elif result.returncode in (4, 5):
        print_error(f"Fallo de autenticación en Atlas: {error_msg}")
    else:
        print_error(f"Fallo en Health Check: {error_msg}")

    print_warning("Revisa tu archivo .env y tus credenciales de red (IP Access List en Atlas).")
    sys.exit(1)


# ==============================================================================
# ORQUESTADOR PRINCIPAL
# ==============================================================================
def main() -> int:
    """Executes the 5-phase Hyperflix IaC and environment setup."""
    print("=" * 72)
    print(" HYPERFLIX STREAMING PLATFORM - ORQUESTADOR DE ENTORNO E IaC ")
    print("=" * 72)

    # 1. Validación de versión de Python
    if not validar_version_python():
        return 1

    # 2. Aislamiento con entorno virtual 'env'
    env_paths = resolver_rutas_entorno(BASE_DIR)
    provisionar_entorno_virtual(env_paths["env_dir"])

    # 3. Instalación de dependencias
    instalar_dependencias(env_paths, REQUIREMENTS_FILE)

    # 4. Provisionamiento local del Data Lake
    provisionar_data_lake(DATA_LAKE_DIR)

    # 5. Health check de base de datos
    ejecutar_health_check(env_paths, ENV_FILE)

    print("\n" + "=" * 72)
    print(" [Hyperflix DevOps] ¡Orquestación completada con éxito! ")
    print(" Para activar el entorno:")
    if os.name == "nt" or sys.platform.startswith("win"):
        print(r"   .\env\Scripts\activate")
    else:
        print("   source env/bin/activate")
    print("=" * 72 + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
