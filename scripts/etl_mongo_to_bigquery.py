#!/usr/bin/env python3
"""Hyperflix ETL Pipeline: MongoDB Atlas (OLTP/DW) -> Google BigQuery (OLAP).

Migracion del modelo estrella analitico de Hyperflix hacia el Data Warehouse
corporativo en Google BigQuery. Separa la base transaccional (MongoDB Atlas /
OLTP) del almacenamiento analitico en la nube (BigQuery / OLAP).

    MongoDB Atlas (hyperflix_db) --Extract--> Pandas --Transform--> BigQuery
            |                                                        |
      modelo estrella OLAP                                  hyperflix_dw.*

Modelo estrella migrado (dominio Hyperflix = streaming de peliculas y series):
    dim_usuario          Suscriptores: perfil, geografia y metricas agregadas
                         de pagos completados (nunca por reproduccion).
    dim_contenido        Catalogo unificado: peliculas + canales de TV.
    dim_tiempo           Calendario UTC de fechas de reproduccion.
    fact_visualizaciones Hecho: una fila por reproduccion con sus claves
                         foraneas y la medida `minutos_reproducidos`.

Reglas de negocio heredadas del Data Warehouse de MongoDB:
    * Los pagos de suscripcion NO se atribuyen a una reproduccion individual:
      `total_pagado` es un agregado a nivel de suscriptor.
    * La tabla de hechos solo contiene claves, claves foraneas y medidas.
    * `reproducciones_analitica` (coleccion OLAP legacy) NO se migra: fue
      reemplazada por el modelo estrella y duplicaria la tabla de hechos.

El pipeline es idempotente: cada ejecucion reescribe las tablas destino
(WRITE_TRUNCATE por defecto) y reconcilia los conteos origen vs destino.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SchemaField
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # pragma: no cover - depende de la consola de Windows
        pass

LOGGER = logging.getLogger("hyperflix_etl")
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_PREFIX = "[Hyperflix ETL]"

# Orden canonico de carga: primero las dimensiones, despues el hecho.
WAREHOUSE_TABLES: tuple[str, ...] = (
    "dim_usuario",
    "dim_contenido",
    "dim_tiempo",
    "fact_visualizaciones",
)

# Clave de negocio (surrogate key) de cada tabla del modelo estrella.
PRIMARY_KEYS: dict[str, str] = {
    "dim_usuario": "usuario_id",
    "dim_contenido": "contenido_id",
    "dim_tiempo": "fecha_id",
    "fact_visualizaciones": "visualizacion_id",
}

# Columnas de clustering declaradas por tabla en BigQuery.
CLUSTERING_FIELDS: dict[str, list[str]] = {
    "dim_usuario": ["pais"],
    "dim_contenido": ["content_type"],
    "dim_tiempo": ["anio"],
    "fact_visualizaciones": ["usuario_id", "contenido_id"],
}

VALID_DISPOSITIONS: tuple[str, ...] = (
    "WRITE_TRUNCATE",
    "WRITE_APPEND",
    "WRITE_EMPTY",
)

# ---------------------------------------------------------------------------
# Esquemas BigQuery canonicos del Data Warehouse de Hyperflix.
# Declararlos de forma explicita hace la carga determinista y permite que
# BigQuery valide el tipado NoSQL -> SQL columnar en cada ejecucion.
# ---------------------------------------------------------------------------
BQ_SCHEMAS: dict[str, list[SchemaField]] = {
    "dim_usuario": [
        SchemaField("usuario_id", "STRING", mode="REQUIRED"),
        SchemaField("codigo_usuario", "STRING"),
        SchemaField("nombre", "STRING"),
        SchemaField("email", "STRING"),
        SchemaField("pais", "STRING"),
        SchemaField("ciudad", "STRING"),
        SchemaField("activo", "BOOLEAN"),
        SchemaField("fecha_registro", "TIMESTAMP"),
        SchemaField("total_pagado", "INTEGER"),
        SchemaField("pagos_completados", "INTEGER"),
    ],
    "dim_contenido": [
        SchemaField("contenido_id", "STRING", mode="REQUIRED"),
        SchemaField("content_type", "STRING"),
        SchemaField("titulo", "STRING"),
        SchemaField("generos", "STRING", mode="REPEATED"),
        SchemaField("categoria", "STRING"),
        SchemaField("anio_lanzamiento", "INTEGER"),
        SchemaField("duracion_minutos", "INTEGER"),
        SchemaField("disponible", "BOOLEAN"),
    ],
    "dim_tiempo": [
        SchemaField("fecha_id", "STRING", mode="REQUIRED"),
        SchemaField("fecha", "TIMESTAMP"),
        SchemaField("anio", "INTEGER"),
        SchemaField("mes", "INTEGER"),
        SchemaField("dia", "INTEGER"),
        SchemaField("dia_semana", "INTEGER"),
        SchemaField("nombre_dia", "STRING"),
        SchemaField("trimestre", "INTEGER"),
        SchemaField("es_fin_de_semana", "BOOLEAN"),
    ],
    "fact_visualizaciones": [
        SchemaField("visualizacion_id", "STRING", mode="REQUIRED"),
        SchemaField("usuario_id", "STRING"),
        SchemaField("contenido_id", "STRING"),
        SchemaField("suscripcion_id", "STRING"),
        SchemaField("fecha_id", "STRING"),
        SchemaField("minutos_reproducidos", "INTEGER"),
        SchemaField("fecha", "DATE"),
    ],
}


class EtlError(RuntimeError):
    """Error funcional del pipeline ETL de Hyperflix."""


class SchemaMismatchError(EtlError):
    """El esquema de la tabla destino no coincide con el esquema canonico."""


@dataclass(frozen=True)
class Settings:
    """Configuracion efectiva del pipeline, resuelta desde `.env`."""

    mongo_uri: str
    mongo_db_name: str
    credentials_path: Path
    bigquery_project_id: str
    bigquery_dataset_id: str
    bigquery_location: str
    batch_size: int
    write_disposition: str
    fact_partitioning: bool = False


def configure_logging(level: str = "INFO") -> None:
    """Configura el logging con el prefijo corporativo de Hyperflix."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=f"{LOG_PREFIX} %(message)s",
    )


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise EtlError(f"Falta la variable de entorno obligatoria: {name}")
    return value


def _project_from_credentials(credentials_path: Path) -> str:
    """Lee el `project_id` del JSON de la cuenta de servicio de BigQuery."""
    try:
        with credentials_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise EtlError(
            f"No se pudo leer el archivo de credenciales de BigQuery: {credentials_path}"
        ) from error
    project_id = str(payload.get("project_id", "")).strip()
    if not project_id:
        raise EtlError(
            f"El archivo de credenciales {credentials_path} no define 'project_id'."
        )
    return project_id


def load_settings() -> Settings:
    """Resuelve la configuracion del ETL desde las variables de entorno."""
    load_dotenv(BASE_DIR / ".env")

    mongo_uri = _required_env("MONGO_URI")
    mongo_db_name = os.getenv("MONGO_DB_NAME", "hyperflix_db").strip() or "hyperflix_db"

    credentials_raw = _required_env("GOOGLE_APPLICATION_CREDENTIALS")
    credentials_path = Path(credentials_raw)
    if not credentials_path.is_absolute():
        credentials_path = (BASE_DIR / credentials_path).resolve()
    if not credentials_path.exists():
        raise EtlError(
            "No se encuentra el archivo de credenciales de BigQuery: "
            f"{credentials_path}. Verifica GOOGLE_APPLICATION_CREDENTIALS en .env."
        )
    # Se exporta la ruta absoluta para que las credenciales aplicativas (ADC)
    # funcionen sin importar el directorio de trabajo actual.
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)

    project_id = os.getenv("BIGQUERY_PROJECT_ID", "").strip()
    if not project_id:
        project_id = _project_from_credentials(credentials_path)

    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "hyperflix_dw").strip() or "hyperflix_dw"
    location = os.getenv("BIGQUERY_LOCATION", "US").strip() or "US"

    batch_size = int(os.getenv("ETL_BATCH_SIZE", os.getenv("MONGO_BATCH_SIZE", "1000")))
    if batch_size <= 0:
        raise EtlError("ETL_BATCH_SIZE debe ser un entero positivo.")

    disposition = os.getenv("ETL_WRITE_DISPOSITION", "WRITE_TRUNCATE").strip().upper()
    if disposition not in VALID_DISPOSITIONS:
        raise EtlError(
            f"ETL_WRITE_DISPOSITION invalido: {disposition}. "
            f"Valores permitidos: {', '.join(VALID_DISPOSITIONS)}."
        )

    fact_partitioning = os.getenv("ETL_FACT_PARTITIONING", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }

    return Settings(
        mongo_uri=mongo_uri,
        mongo_db_name=mongo_db_name,
        credentials_path=credentials_path,
        bigquery_project_id=project_id,
        bigquery_dataset_id=dataset_id,
        bigquery_location=location,
        batch_size=batch_size,
        write_disposition=disposition,
        fact_partitioning=fact_partitioning,
    )


def get_mongo_client(settings: Settings) -> MongoClient:
    """Abre la conexion de solo lectura contra MongoDB Atlas."""
    try:
        client = MongoClient(
            settings.mongo_uri,
            serverSelectionTimeoutMS=10_000,
            connectTimeoutMS=10_000,
            appname="hyperflix-etl-bigquery",
        )
        client.admin.command("ping")
        return client
    except PyMongoError as error:
        raise EtlError("No se pudo conectar a MongoDB Atlas (hyperflix_db).") from error


def get_bigquery_client(settings: Settings) -> bigquery.Client:
    """Abre el cliente de BigQuery usando las credenciales de la cuenta de servicio."""
    try:
        return bigquery.Client(project=settings.bigquery_project_id)
    except GoogleAPIError as error:
        raise EtlError(
            "No se pudieron autenticar las credenciales de BigQuery. Verifica que la "
            f"cuenta de servicio tenga los roles BigQuery Data Editor y Job User. "
            f"Detalle: {error}"
        ) from error
    except Exception as error:  # google.auth no hereda de GoogleAPIError
        raise EtlError(
            f"No se pudieron autenticar las credenciales de BigQuery: {error}"
        ) from error


# ============================================================================
# FASE 1 - EXTRACT
# ============================================================================
def extract_collection(
    database: Database, collection_name: str, batch_size: int
) -> pd.DataFrame:
    """Extrae la totalidad de una coleccion analitica de MongoDB a un DataFrame.

    Se conservan los documentos BSON sin aplanar para que las reglas de
    limpieza (por ejemplo el envoltorio `ifNull` de `dim_usuario`) vivan en la
    fase de transformacion, donde son verificables con tests unitarios.
    """
    documents: list[dict[str, Any]] = []
    for document in database[collection_name].find(batch_size=batch_size):
        documents.append(document)
    if not documents:
        LOGGER.warning("La coleccion %s no devolvio documentos.", collection_name)
        return pd.DataFrame()
    return pd.DataFrame.from_records(documents)


# ============================================================================
# FASE 2 - TRANSFORM (tipado de datos NoSQL -> SQL columnar)
# ============================================================================
def _column(frame: pd.DataFrame, name: str) -> pd.Series:
    """Devuelve la columna `name` o una serie nula del mismo largo."""
    if name in frame.columns:
        return frame[name]
    return pd.Series([None] * len(frame), index=frame.index, dtype="object")


def _bson_to_string(value: Any) -> str | None:
    """Convierte un ObjectId de MongoDB a su representacion String."""
    if value is None:
        return None
    if isinstance(value, ObjectId):
        return str(value)
    return str(value)


def _unwrap_ifnull(value: Any) -> Any:
    """Desempaqueta el documento literal `{"ifNull": [valor, defecto]}`.

    El constructor del Data Warehouse (scripts/data_warehouse_builder.py)
    escribe `{"ifNull": [...]}` dentro de un `$project`. MongoDB no lo
    interpreta como el operador `$ifNull` (que requiere el prefijo `$`), por lo
    que el valor queda guardado como subdocumento literal. El ETL normaliza
    ambos casos sin modificar el warehouse de origen.
    """
    if isinstance(value, dict) and "ifNull" in value:
        arguments = value.get("ifNull")
        if isinstance(arguments, (list, tuple)) and arguments:
            return arguments[0]
        return None
    return value


def _bson_series(frame: pd.DataFrame, *candidates: str) -> pd.Series:
    """Devuelve la primera columna candidata presente, convertida a STRING."""
    for name in candidates:
        if name in frame.columns:
            return _column(frame, name).map(_bson_to_string)
    return pd.Series([None] * len(frame), index=frame.index, dtype="object")


def _text_series(frame: pd.DataFrame, name: str) -> pd.Series:
    """Columna de texto con `None` preservado como valor nulo."""
    if name not in frame.columns:
        return pd.Series([None] * len(frame), index=frame.index, dtype="object")
    return frame[name].map(lambda value: None if value is None else str(value))


def _int_series(frame: pd.DataFrame, name: str) -> pd.Series:
    """Columna entera nullable (`Int64`), tolerante a nulos y a valores envueltos."""
    if name not in frame.columns:
        return pd.Series([], index=frame.index, dtype="Int64")
    values = frame[name].map(_unwrap_ifnull)
    return pd.to_numeric(values, errors="coerce").astype("Int64")


def _bool_series(frame: pd.DataFrame, name: str) -> pd.Series:
    """Columna booleana nullable (`boolean`), tolerante a nulos."""
    if name not in frame.columns:
        return pd.Series([], index=frame.index, dtype="boolean")

    def to_bool(value: Any) -> Any:
        if value is None or value is pd.NA:
            return pd.NA
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "t", "1", "yes", "y"}:
                return True
            if normalized in {"false", "f", "0", "no", "n"}:
                return False
        return pd.NA

    return frame[name].map(to_bool).astype("boolean")


def _utc_series(frame: pd.DataFrame, name: str) -> pd.Series:
    """Columna de marca temporal en UTC (`datetime64[ns, UTC]`)."""
    if name not in frame.columns:
        return pd.Series([], index=frame.index, dtype="datetime64[ns, UTC]")
    return pd.to_datetime(frame[name], utc=True, errors="coerce")


def _array_series(frame: pd.DataFrame, name: str) -> pd.Series:
    """Columna `ARRAY<STRING>`: listas de texto, nunca nulas (lista vacia si no hay)."""
    if name not in frame.columns:
        return pd.Series([[] for _ in range(len(frame))], index=frame.index, dtype="object")

    def to_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            return [str(item) for item in value if item is not None]
        if isinstance(value, str):
            return [value] if value else []
        return [str(value)]

    return frame[name].map(to_list)


def transform_dim_usuario(frame: pd.DataFrame) -> pd.DataFrame:
    """Transforma `dim_usuario`: perfil del suscriptor y metricas de pago agregado.

    Los pagos de suscripcion se agregan por suscriptor (nunca por
    reproduccion) y llegan envueltos en el documento literal `ifNull`.
    """
    data = pd.DataFrame(index=frame.index)
    data["usuario_id"] = _bson_series(frame, "usuario_id", "_id")
    data["codigo_usuario"] = _text_series(frame, "codigo_usuario")
    data["nombre"] = _text_series(frame, "nombre")
    data["email"] = _text_series(frame, "email")
    data["pais"] = _text_series(frame, "pais")
    data["ciudad"] = _text_series(frame, "ciudad")
    data["activo"] = _bool_series(frame, "activo")
    data["fecha_registro"] = _utc_series(frame, "fecha_registro")
    data["total_pagado"] = _int_series(frame, "total_pagado")
    data["pagos_completados"] = _int_series(frame, "pagos_completados")
    return data


def transform_dim_contenido(frame: pd.DataFrame) -> pd.DataFrame:
    """Transforma `dim_contenido`: catalogo unificado de peliculas y canales de TV.

    Las peliculas traen `generos` (ARRAY<STRING>) y las categorias de canal,
    mientras que los canales de TV no tienen anio de estreno ni duracion.
    """
    data = pd.DataFrame(index=frame.index)
    data["contenido_id"] = _bson_series(frame, "contenido_id", "_id")
    data["content_type"] = _text_series(frame, "content_type")
    data["titulo"] = _text_series(frame, "titulo")
    data["generos"] = _array_series(frame, "generos")
    data["categoria"] = _text_series(frame, "categoria")
    data["anio_lanzamiento"] = _int_series(frame, "anio_lanzamiento")
    data["duracion_minutos"] = _int_series(frame, "duracion_minutos")
    data["disponible"] = _bool_series(frame, "disponible")
    return data


def transform_dim_tiempo(frame: pd.DataFrame) -> pd.DataFrame:
    """Transforma `dim_tiempo`: calendario UTC de fechas de reproduccion.

    El `_id` de esta dimension es la propia clave de negocio (`fecha_id`),
    por lo que se unifica en una sola columna.
    """
    data = pd.DataFrame(index=frame.index)
    data["fecha_id"] = _bson_series(frame, "fecha_id", "_id")
    data["fecha"] = _utc_series(frame, "fecha")
    data["anio"] = _int_series(frame, "anio")
    data["mes"] = _int_series(frame, "mes")
    data["dia"] = _int_series(frame, "dia")
    data["dia_semana"] = _int_series(frame, "dia_semana")
    data["nombre_dia"] = _text_series(frame, "nombre_dia")
    data["trimestre"] = _int_series(frame, "trimestre")
    data["es_fin_de_semana"] = _bool_series(frame, "es_fin_de_semana")
    return data


def transform_fact_visualizaciones(
    frame: pd.DataFrame, fecha_lookup: dict[str, date] | None = None
) -> pd.DataFrame:
    """Transforma `fact_visualizaciones`: una fila por reproduccion de Hyperflix.

    Solo se conservan claves, claves foraneas y la medida `minutos_reproducidos`.
    La columna `fecha` se deriva de `dim_tiempo` (join 1:1 por `fecha_id`) para
    habilitar el particionado por fecha en BigQuery sin alterar la granularidad.
    """
    data = pd.DataFrame(index=frame.index)
    data["visualizacion_id"] = _bson_series(frame, "visualizacion_id", "_id")
    data["usuario_id"] = _bson_series(frame, "usuario_id")
    data["contenido_id"] = _bson_series(frame, "contenido_id")
    data["suscripcion_id"] = _bson_series(frame, "suscripcion_id")
    data["fecha_id"] = _text_series(frame, "fecha_id")
    data["minutos_reproducidos"] = _int_series(frame, "minutos_reproducidos")
    if fecha_lookup:
        data["fecha"] = data["fecha_id"].map(fecha_lookup)
        sin_fecha = int(data["fecha"].isna().sum())
        if sin_fecha:
            LOGGER.warning(
                "  %d reproducciones sin fecha correlacionada en dim_tiempo.",
                sin_fecha,
            )
    else:
        data["fecha"] = pd.Series([None] * len(data), index=data.index, dtype="object")
    return data


def build_fecha_lookup(dim_tiempo: pd.DataFrame) -> dict[str, date]:
    """Construye el mapa `fecha_id -> fecha` a partir de la dimension de tiempo."""
    if dim_tiempo.empty or "fecha_id" not in dim_tiempo.columns:
        return {}
    lookup: dict[str, date] = {}
    for fecha_id, fecha in zip(dim_tiempo["fecha_id"], dim_tiempo["fecha"]):
        if fecha_id is None or fecha is None or pd.isna(fecha):
            continue
        lookup[str(fecha_id)] = fecha.date()
    return lookup


TRANSFORMERS = {
    "dim_usuario": transform_dim_usuario,
    "dim_contenido": transform_dim_contenido,
    "dim_tiempo": transform_dim_tiempo,
}


def transform_table(
    table: str, frame: pd.DataFrame, fecha_lookup: dict[str, date] | None = None
) -> pd.DataFrame:
    """Aplica la transformacion canonica de una tabla del modelo estrella."""
    if table == "fact_visualizaciones":
        data = transform_fact_visualizaciones(frame, fecha_lookup)
    else:
        transformer = TRANSFORMERS.get(table)
        if transformer is None:
            raise EtlError(f"No existe transformacion definida para la tabla {table}.")
        data = transformer(frame)

    # La clave de negocio es REQUIRED en BigQuery: se valida antes de cargar.
    key = PRIMARY_KEYS.get(table)
    if key is None:
        raise EtlError(f"No hay clave de negocio definida para la tabla {table}.")
    if key not in data.columns:
        raise EtlError(
            f"La transformacion de {table} no produjo la columna de clave {key}."
        )
    sin_clave = int(data[key].isna().sum())
    if sin_clave:
        raise EtlError(
            f"La tabla {table} tiene {sin_clave} filas sin clave de negocio {key}; "
            "BigQuery no acepta nulos en columnas REQUIRED."
        )
    return data


# ============================================================================
# FASE 3 - LOAD
# ============================================================================
def ensure_dataset(client: bigquery.Client, settings: Settings) -> str:
    """Verifica (o crea) el dataset destino y devuelve su ubicacion."""
    dataset_ref = client.dataset(settings.bigquery_dataset_id)
    try:
        dataset = client.get_dataset(dataset_ref)
        LOGGER.info(
            "Dataset BigQuery verificado: %s.%s (ubicacion: %s)",
            settings.bigquery_project_id,
            settings.bigquery_dataset_id,
            dataset.location,
        )
        return dataset.location
    except NotFound:
        LOGGER.info(
            "Dataset %s inexistente: creando en la ubicacion %s...",
            settings.bigquery_dataset_id,
            settings.bigquery_location,
        )
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = settings.bigquery_location
        created = client.create_dataset(dataset)
        LOGGER.info("Dataset creado: %s", settings.bigquery_dataset_id)
        return created.location


def _schema_signature(schema: Sequence[SchemaField]) -> list[tuple[str, str, str]]:
    """Firma comparable de un esquema BigQuery (nombre, tipo, modo)."""
    return [
        (field.name, field.field_type.upper(), (field.mode or "NULLABLE").upper())
        for field in schema
    ]


def _desired_partitioning(
    table: str, enabled: bool = False
) -> bigquery.TimePartitioning | None:
    """Particionado canonico por tabla (rango diario de fecha en el hecho).

    El particionado es opt-in (`ETL_FACT_PARTITIONING=true`) porque BigQuery en
    modo sandbox, sin billing activa, aplica automaticamente una expiracion de
    particiones de 60 dias al crear la tabla. Con el historico de
    reproducciones de Hyperflix (368 dias) eso eliminaria la mayor parte de las
    particiones recien cargadas. Con billing activa el particionado se puede
    activar sin riesgo.
    """
    if enabled and table == "fact_visualizaciones":
        return bigquery.TimePartitioning(
            field="fecha", type_=bigquery.TimePartitioningType.DAY
        )
    return None


def _align_table_layout(
    client: bigquery.Client,
    table: bigquery.Table,
    name: str,
    partitioning_enabled: bool = False,
) -> bigquery.Table:
    """Aplica el particionado y el clustering canonicos si la tabla no los tiene.

    BigQuery en modo sandbox (sin billing activa) rechaza la modificacion de
    tablas existentes. En ese caso la distribucion fisica se reporta como
    advertencia y el pipeline continua: es una optimizacion, no un requisito
    de correctitud de los datos.
    """
    updates: list[str] = []

    desired_partitioning = _desired_partitioning(name, partitioning_enabled)
    current_partitioning = table.time_partitioning
    if desired_partitioning is not None and (
        current_partitioning is None
        or current_partitioning.field != desired_partitioning.field
    ):
        table.time_partitioning = desired_partitioning
        updates.append("time_partitioning")

    desired_clustering = CLUSTERING_FIELDS.get(name, [])
    if desired_clustering and list(table.clustering_fields or []) != desired_clustering:
        table.clustering_fields = desired_clustering
        updates.append("clustering_fields")

    if updates:
        LOGGER.warning(
            "Ajustando la distribucion fisica de %s: %s", table.table_id, ", ".join(updates)
        )
        try:
            table = client.update_table(table, updates)
        except GoogleAPIError as error:
            LOGGER.warning(
                "No se pudo ajustar la distribucion fisica de %s: %s. BigQuery en modo "
                "sandbox restringe la modificacion de tablas; ejecuta con "
                "--recreate-tables para aplicarla.",
                table.table_id,
                str(error)[:200],
            )
    return table


def ensure_table(
    client: bigquery.Client, settings: Settings, table: str, recreate: bool = False
) -> bigquery.Table:
    """Garantiza que la tabla destino exista con el esquema canonico de Hyperflix."""
    table_ref = client.dataset(settings.bigquery_dataset_id).table(table)
    canonical = BQ_SCHEMAS[table]

    try:
        existing = client.get_table(table_ref)
    except NotFound:
        existing = None

    if existing is not None:
        if _schema_signature(existing.schema) != _schema_signature(canonical):
            if not recreate:
                raise SchemaMismatchError(
                    f"El esquema de {table_ref} no coincide con el esquema canonico de "
                    "Hyperflix. Ejecuta con --recreate-tables para recrearla."
                )
            LOGGER.warning("Esquema incompatible en %s: recreando la tabla.", table_ref)
            client.delete_table(table_ref, not_found_ok=True)
        elif recreate:
            LOGGER.warning(
                "Recreando la tabla %s para aplicar el esquema y la distribucion "
                "fisica canonicos.",
                table_ref,
            )
            client.delete_table(table_ref, not_found_ok=True)
        else:
            LOGGER.info("Tabla destino verificada: %s", table_ref)
            return _align_table_layout(
                client, existing, table, settings.fact_partitioning
            )

    new_table = bigquery.Table(table_ref, schema=canonical)
    new_table.time_partitioning = _desired_partitioning(table, settings.fact_partitioning)
    clustering = CLUSTERING_FIELDS.get(table)
    if clustering:
        new_table.clustering_fields = clustering
    created = client.create_table(new_table)
    LOGGER.info("Tabla destino creada: %s", table_ref)
    return created


def load_table(
    client: bigquery.Client,
    settings: Settings,
    table: str,
    frame: pd.DataFrame,
    disposition: str,
    location: str,
) -> int:
    """Carga un DataFrame en la tabla destino usando el esquema canonico.

    El formato de origen es PARQUET (por defecto de la libreria) porque es el
    unico que soporta campos REPEATED como `generos` (ARRAY<STRING>).
    """
    if frame.empty:
        LOGGER.warning("Tabla %s sin filas: se omite la carga.", table)
        return 0

    job_config = LoadJobConfig(
        write_disposition=disposition,
        schema=BQ_SCHEMAS[table],
        source_format=bigquery.SourceFormat.PARQUET,
    )
    destination = f"{settings.bigquery_project_id}.{settings.bigquery_dataset_id}.{table}"
    job = client.load_table_from_dataframe(
        frame,
        destination,
        job_config=job_config,
        location=location,
    )
    job.result()
    return int(job.output_rows or 0)


# ============================================================================
# VERIFICACION - Reconciliacion origen vs destino
# ============================================================================
def reconcile(
    client: bigquery.Client, settings: Settings, expected: dict[str, int]
) -> list[dict[str, Any]]:
    """Compara el conteo de documentos en Atlas contra las filas en BigQuery."""
    results: list[dict[str, Any]] = []
    for table, source_rows in expected.items():
        destination = f"{settings.bigquery_project_id}.{settings.bigquery_dataset_id}.{table}"
        rows = list(client.query(f"SELECT COUNT(*) AS total FROM `{destination}`").result())
        target_rows = int(rows[0]["total"]) if rows else 0
        estado = "OK" if target_rows == source_rows else "MISMATCH"
        results.append(
            {
                "tabla": table,
                "origen": source_rows,
                "destino": target_rows,
                "estado": estado,
            }
        )
        LOGGER.info(
            "Reconciliacion %-24s Atlas=%-7d BigQuery=%-7d %s",
            table,
            source_rows,
            target_rows,
            estado,
        )
    return results


def check_referential_integrity(
    client: bigquery.Client, settings: Settings
) -> dict[str, int]:
    """Verifica que el hecho no referencie dimensiones inexistentes."""
    base = f"{settings.bigquery_project_id}.{settings.bigquery_dataset_id}"
    fact = f"`{base}.fact_visualizaciones`"
    queries = {
        "fechas_huerfanas": f"SELECT COUNT(*) AS total FROM {fact} f WHERE f.fecha_id NOT IN (SELECT fecha_id FROM `{base}.dim_tiempo`)",
        "usuarios_huerfanos": f"SELECT COUNT(*) AS total FROM {fact} f WHERE f.usuario_id NOT IN (SELECT usuario_id FROM `{base}.dim_usuario`)",
        "contenidos_huerfanos": f"SELECT COUNT(*) AS total FROM {fact} f WHERE f.contenido_id NOT IN (SELECT contenido_id FROM `{base}.dim_contenido`)",
    }
    integrity: dict[str, int] = {}
    for name, query in queries.items():
        try:
            rows = list(client.query(query).result())
        except GoogleAPIError as error:
            LOGGER.warning("No se pudo verificar %s: %s", name, error)
            continue
        total = int(rows[0]["total"]) if rows else 0
        integrity[name] = total
        LOGGER.info("Integridad referencial %-22s %d", name, total)
    return integrity


# ============================================================================
# ORQUESTADOR DEL PIPELINE
# ============================================================================
def run_pipeline(
    settings: Settings,
    tables: Sequence[str] = WAREHOUSE_TABLES,
    dry_run: bool = False,
    disposition: str | None = None,
    recreate: bool = False,
) -> dict[str, Any]:
    """Ejecuta el pipeline ETL completo de Hyperflix (Extract -> Transform -> Load)."""
    run_id = uuid.uuid4().hex[:12]
    started = time.perf_counter()
    selected = [table for table in WAREHOUSE_TABLES if table in set(tables)]
    write_disposition = (disposition or settings.write_disposition).upper()

    LOGGER.info("Iniciando pipeline ETL Hyperflix -> BigQuery (run_id=%s)", run_id)
    LOGGER.info(
        "Origen: MongoDB Atlas / %s | Destino: BigQuery %s.%s",
        settings.mongo_db_name,
        settings.bigquery_project_id,
        settings.bigquery_dataset_id,
    )
    LOGGER.info("Tablas del modelo estrella a migrar: %s", ", ".join(selected))

    mongo_client = get_mongo_client(settings)
    try:
        database = mongo_client[settings.mongo_db_name]
        available = set(database.list_collection_names())
        missing = [table for table in selected if table not in available]
        if missing:
            raise EtlError(
                "Faltan colecciones analiticas en MongoDB Atlas: "
                f"{', '.join(missing)}. Ejecuta scripts/data_warehouse_builder.py primero."
            )

        # La dimension de tiempo se necesita siempre para correlacionar la fecha
        # del hecho, incluso cuando no se va a recargar.
        extract_set = set(selected)
        if "fact_visualizaciones" in selected:
            extract_set.add("dim_tiempo")

        raw_frames: dict[str, pd.DataFrame] = {}
        source_rows: dict[str, int] = {}
        for table in WAREHOUSE_TABLES:
            if table not in extract_set:
                continue
            LOGGER.info("Extrayendo %s de MongoDB (%s)...", table, settings.mongo_db_name)
            raw = extract_collection(database, table, settings.batch_size)
            LOGGER.info("  %s: %d documentos extraidos de Atlas.", table, len(raw))
            raw_frames[table] = raw
            source_rows[table] = len(raw)

        LOGGER.info(
            "Transformando tipos de datos (ObjectId -> STRING, ifNull -> INTEGER, "
            "fechas -> UTC, generos -> ARRAY<STRING>)..."
        )
        transformed: dict[str, pd.DataFrame] = {}
        fecha_lookup: dict[str, date] = {}
        for table in WAREHOUSE_TABLES:
            if table not in extract_set:
                continue
            data = transform_table(table, raw_frames[table], fecha_lookup)
            transformed[table] = data
            LOGGER.info(
                "  %s: %d filas transformadas | %d columnas de destino",
                table,
                len(data),
                len(data.columns),
            )
            if table == "dim_tiempo":
                fecha_lookup = build_fecha_lookup(data)

        if dry_run:
            LOGGER.info("Modo --dry-run: no se escribe informacion en BigQuery.")
            for table in WAREHOUSE_TABLES:
                if table in transformed:
                    LOGGER.info(
                        "  %-24s %6d filas -> %s.%s",
                        table,
                        len(transformed[table]),
                        settings.bigquery_dataset_id,
                        table,
                    )
            LOGGER.info(
                "Simulacion completada en %.2f segundos.", time.perf_counter() - started
            )
            return {
                "run_id": run_id,
                "dry_run": True,
                "tables": {table: len(frame) for table, frame in transformed.items()},
            }

        bq_client = get_bigquery_client(settings)
        location = ensure_dataset(bq_client, settings)

        for table in WAREHOUSE_TABLES:
            if table not in selected:
                continue
            ensure_table(bq_client, settings, table, recreate=recreate)
            rows = load_table(
                bq_client, settings, table, transformed[table], write_disposition, location
            )
            LOGGER.info(
                "Carga exitosa en BigQuery: %s.%s (%d filas)",
                settings.bigquery_dataset_id,
                table,
                rows,
            )

        reconciliation = reconcile(
            bq_client, settings, {table: source_rows[table] for table in selected}
        )
        integrity = check_referential_integrity(bq_client, settings)
        elapsed = time.perf_counter() - started
        LOGGER.info("Pipeline ETL completado en %.2f segundos.", elapsed)

        mismatches = [item for item in reconciliation if item["estado"] != "OK"]
        if mismatches:
            detalle = ", ".join(
                f"{item['tabla']} (Atlas={item['origen']} vs BigQuery={item['destino']})"
                for item in mismatches
            )
            raise EtlError(
                f"La reconciliacion de conteos fallo: {detalle}. Si la tabla de hechos "
                "esta particionada, ten en cuenta que BigQuery en modo sandbox expira "
                "automaticamente las particiones con mas de 60 dias: ejecuta con "
                "--recreate-tables y ETL_FACT_PARTITIONING=false."
            )
        if any(integrity.values()):
            LOGGER.warning(
                "Se detectaron referencias huerfanas en la tabla de hechos: %s", integrity
            )
        return {
            "run_id": run_id,
            "dry_run": False,
            "tables": {table: len(frame) for table, frame in transformed.items()},
            "reconciliation": reconciliation,
            "integrity": integrity,
            "elapsed_seconds": round(elapsed, 2),
        }
    finally:
        mongo_client.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Define la interfaz de linea de comandos del pipeline ETL."""
    parser = argparse.ArgumentParser(
        description=(
            "Pipeline ETL de Hyperflix: migra el modelo estrella de MongoDB Atlas "
            "hacia Google BigQuery (dataset hyperflix_dw)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  python scripts/etl_mongo_to_bigquery.py --dry-run\n"
            "  python scripts/etl_mongo_to_bigquery.py\n"
            "  python scripts/etl_mongo_to_bigquery.py --only dim_usuario,dim_contenido\n"
            "  python scripts/etl_mongo_to_bigquery.py --recreate-tables\n"
        ),
    )
    parser.add_argument(
        "--only",
        default="",
        help=(
            "Lista separada por comas de tablas a migrar "
            f"({', '.join(WAREHOUSE_TABLES)}). Por defecto: todas."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extrae y transforma sin escribir nada en BigQuery.",
    )
    parser.add_argument(
        "--write-disposition",
        choices=list(VALID_DISPOSITIONS),
        default=None,
        help="Comportamiento de escritura en BigQuery (por defecto: WRITE_TRUNCATE).",
    )
    parser.add_argument(
        "--recreate-tables",
        action="store_true",
        help=(
            "Recrea las tablas destino para aplicar el esquema y la distribucion "
            "fisica canonicos (elimina los datos existentes antes de recargar)."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de detalle del log del pipeline.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Punto de entrada del pipeline ETL de Hyperflix."""
    args = parse_args(argv)
    configure_logging(args.log_level)
    try:
        settings = load_settings()
        tables = [item.strip() for item in args.only.split(",") if item.strip()]
        if not tables:
            tables = list(WAREHOUSE_TABLES)
        unknown = [table for table in tables if table not in WAREHOUSE_TABLES]
        if unknown:
            raise EtlError(
                f"Tablas desconocidas: {', '.join(unknown)}. "
                f"Disponibles: {', '.join(WAREHOUSE_TABLES)}."
            )
        summary = run_pipeline(
            settings,
            tables=tables,
            dry_run=args.dry_run,
            disposition=args.write_disposition,
            recreate=args.recreate_tables,
        )
        LOGGER.info("Resumen del pipeline: %s", summary)
        return 0
    except (EtlError, PyMongoError, GoogleAPIError, OSError, ValueError) as error:
        LOGGER.error("ERROR: el pipeline ETL de Hyperflix finalizo con fallo: %s", error)
        return 1


if __name__ == "__main__":
    sys.exit(main())

