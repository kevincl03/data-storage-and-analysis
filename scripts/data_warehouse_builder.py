"""Build the Hyperflix dimensional warehouse in MongoDB Atlas.

The source collections remain operational. Only the four warehouse collections
managed by this module are rebuilt. Every aggregation explicitly enables disk
use so the same pipelines can handle larger datasets later.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError


LOGGER = logging.getLogger("data_warehouse")
WAREHOUSE_COLLECTIONS = (
    "dim_tiempo",
    "dim_usuario",
    "dim_contenido",
    "fact_visualizaciones",
)
SOURCE_COLLECTIONS = (
    "reproducciones",
    "usuarios",
    "peliculas",
    "canales_tv",
    "suscripciones",
    "pagos",
)


def load_settings() -> tuple[str, str]:
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "").strip()
    database_name = os.getenv("MONGO_DB_NAME", "hyperflix_db").strip()
    if not mongo_uri:
        raise ValueError("Missing required environment variable: MONGO_URI")
    if not database_name:
        raise ValueError("MONGO_DB_NAME cannot be empty")
    return mongo_uri, database_name


def get_client(mongo_uri: str) -> MongoClient:
    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=5_000,
        connectTimeoutMS=5_000,
        appname="hyperflix-data-warehouse",
    )
    client.admin.command("ping")
    return client


def _merge(collection_name: str) -> dict[str, Any]:
    return {
        "$merge": {
            "into": collection_name,
            "on": "_id",
            "whenMatched": "replace",
            "whenNotMatched": "insert",
        }
    }


def _run(collection: Collection, pipeline: list[dict[str, Any]]) -> None:
    list(collection.aggregate(pipeline, allowDiskUse=True))


def validate_source_schema(database: Database) -> None:
    available = set(database.list_collection_names())
    missing = sorted(set(SOURCE_COLLECTIONS) - available)
    if missing:
        raise ValueError(f"Missing source collections: {', '.join(missing)}")


def reset_warehouse(database: Database) -> None:
    for collection_name in WAREHOUSE_COLLECTIONS:
        database[collection_name].drop()
        LOGGER.info("Reset warehouse collection: %s", collection_name)


def build_time_dimension(database: Database) -> None:
    pipeline = [
        {"$match": {"fecha_inicio": {"$type": "date"}}},
        {
            "$project": {
                "date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$fecha_inicio",
                        "timezone": "UTC",
                    }
                }
            }
        },
        {"$group": {"_id": "$date"}},
        {
            "$set": {
                "fecha": {"$dateFromString": {"dateString": "$_id", "timezone": "UTC"}},
                "fecha_id": {"$replaceAll": {"input": "$_id", "find": "-", "replacement": ""}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "fecha_id": 1,
                "fecha": 1,
                "anio": {"$year": "$fecha"},
                "mes": {"$month": "$fecha"},
                "dia": {"$dayOfMonth": "$fecha"},
                "dia_semana": {"$dayOfWeek": "$fecha"},
                "nombre_dia": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": [{"$dayOfWeek": "$fecha"}, 1]}, "then": "domingo"},
                            {"case": {"$eq": [{"$dayOfWeek": "$fecha"}, 2]}, "then": "lunes"},
                            {"case": {"$eq": [{"$dayOfWeek": "$fecha"}, 3]}, "then": "martes"},
                            {"case": {"$eq": [{"$dayOfWeek": "$fecha"}, 4]}, "then": "miercoles"},
                            {"case": {"$eq": [{"$dayOfWeek": "$fecha"}, 5]}, "then": "jueves"},
                            {"case": {"$eq": [{"$dayOfWeek": "$fecha"}, 6]}, "then": "viernes"},
                            {"case": {"$eq": [{"$dayOfWeek": "$fecha"}, 7]}, "then": "sabado"},
                        ],
                        "default": "desconocido",
                    }
                },
                "trimestre": {"$ceil": {"$divide": [{"$month": "$fecha"}, 3]}},
                "es_fin_de_semana": {"$in": [{"$dayOfWeek": "$fecha"}, [1, 7]]},
            }
        },
        {"$set": {"_id": "$fecha_id"}},
        _merge("dim_tiempo"),
    ]
    _run(database["reproducciones"], pipeline)


def build_user_dimension(database: Database) -> None:
    pipeline = [
        {
            "$lookup": {
                "from": "pagos",
                "let": {"user_id": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$and": [{"$eq": ["$usuario_id", "$$user_id"]}, {"$eq": ["$estado", "Completado"]}]}}},
                    {"$group": {"_id": None, "total_pagado": {"$sum": "$monto"}, "pagos_completados": {"$sum": 1}}},
                ],
                "as": "payment_summary",
            }
        },
        {
            "$project": {
                "_id": 1,
                "usuario_id": "$_id",
                "codigo_usuario": 1,
                "nombre": 1,
                "email": 1,
                "pais": 1,
                "ciudad": 1,
                "activo": 1,
                "fecha_registro": 1,
                "total_pagado": {"ifNull": [{"$arrayElemAt": ["$payment_summary.total_pagado", 0]}, 0]},
                "pagos_completados": {"ifNull": [{"$arrayElemAt": ["$payment_summary.pagos_completados", 0]}, 0]},
            }
        },
        _merge("dim_usuario"),
    ]
    _run(database["usuarios"], pipeline)


def build_content_dimension(database: Database) -> None:
    pipeline = [
        {
            "$project": {
                "_id": 1,
                "contenido_id": "$_id",
                "content_type": {"$literal": "pelicula"},
                "titulo": 1,
                "generos": 1,
                "categoria": {"$literal": None},
                "anio_lanzamiento": "$anio_estreno",
                "duracion_minutos": 1,
                "disponible": 1,
            }
        },
        {"$unionWith": {"coll": "canales_tv", "pipeline": [
            {
                "$project": {
                    "_id": 1,
                    "contenido_id": "$_id",
                    "content_type": {"$literal": "canal_tv"},
                    "titulo": "$nombre",
                    "generos": {"$literal": []},
                    "categoria": 1,
                    "anio_lanzamiento": {"$literal": None},
                    "duracion_minutos": {"$literal": None},
                    "disponible": "$activo",
                }
            }
        ]}},
        {"$set": {"_id": "$contenido_id"}},
        _merge("dim_contenido"),
    ]
    _run(database["peliculas"], pipeline)


def build_visualization_fact(database: Database) -> None:
    pipeline = [
        {"$match": {"fecha_inicio": {"$type": "date"}}},
        {
            "$project": {
                "_id": 1,
                "visualizacion_id": "$_id",
                "fecha_id": {
                    "$replaceAll": {
                        "input": {"$dateToString": {"format": "%Y-%m-%d", "date": "$fecha_inicio", "timezone": "UTC"}},
                        "find": "-",
                        "replacement": "",
                    }
                },
                "usuario_id": 1,
                "contenido_id": 1,
                "suscripcion_id": 1,
                "minutos_reproducidos": {"$ifNull": ["$duracion_vista_minutos", 0]},
            }
        },
        _merge("fact_visualizaciones"),
    ]
    _run(database["reproducciones"], pipeline)


def create_indexes(database: Database) -> None:
    database["dim_tiempo"].create_index("fecha_id", unique=True, name="uk_dim_tiempo_fecha")
    database["dim_usuario"].create_index("usuario_id", unique=True, name="uk_dim_usuario_id")
    database["dim_contenido"].create_index("contenido_id", unique=True, name="uk_dim_contenido_id")
    database["fact_visualizaciones"].create_index("visualizacion_id", unique=True, name="uk_fact_visualizacion_id")
    database["fact_visualizaciones"].create_index([("fecha_id", 1), ("usuario_id", 1), ("contenido_id", 1)], name="idx_fact_star_keys")


def validate_warehouse(database: Database) -> dict[str, int]:
    counts = {name: database[name].count_documents({}) for name in WAREHOUSE_COLLECTIONS}
    fact_count = counts["fact_visualizaciones"]
    orphan_dates = database["fact_visualizaciones"].count_documents({"fecha_id": {"$nin": database["dim_tiempo"].distinct("fecha_id")}}) if fact_count else 0
    orphan_users = database["fact_visualizaciones"].count_documents({"usuario_id": {"$nin": database["dim_usuario"].distinct("usuario_id")}}) if fact_count else 0
    orphan_content = database["fact_visualizaciones"].count_documents({"contenido_id": {"$nin": database["dim_contenido"].distinct("contenido_id")}}) if fact_count else 0
    if orphan_dates or orphan_users or orphan_content:
        raise ValueError(f"Warehouse referential integrity failed: dates={orphan_dates}, users={orphan_users}, content={orphan_content}")
    for name, count in counts.items():
        LOGGER.info("WAREHOUSE %-22s documents=%s", name, count)
    return counts


def build_data_warehouse() -> dict[str, int]:
    mongo_uri, database_name = load_settings()
    client = get_client(mongo_uri)
    try:
        database = client[database_name]
        validate_source_schema(database)
        reset_warehouse(database)
        build_time_dimension(database)
        build_user_dimension(database)
        build_content_dimension(database)
        build_visualization_fact(database)
        create_indexes(database)
        counts = validate_warehouse(database)
        print("DATA WAREHOUSE CONFIGURED SUCCESSFULLY")
        return counts
    finally:
        client.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    try:
        build_data_warehouse()
    except (ValueError, PyMongoError, OSError) as error:
        LOGGER.error("Data Warehouse build failed: %s", error)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()