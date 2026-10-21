"""Safe Hyperflix activity producer for Change Stream demonstrations."""

from __future__ import annotations

import argparse
import logging
import random
import time
from datetime import datetime, timedelta, timezone

from bson import ObjectId
from pymongo import MongoClient

from streaming_common import load_streaming_settings


LOGGER = logging.getLogger("generar_eventos_streaming")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate Hyperflix CDC activity")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between generated events")
    parser.add_argument("--once", action="store_true", help="Generate one insert, update and safe delete")
    return parser


def load_reference_data(database):
    user = database["usuarios"].find_one({}, {"_id": 1})
    content = database["peliculas"].find_one({}, {"_id": 1})
    subscription = database["suscripciones"].find_one({}, {"_id": 1, "usuario_id": 1, "plan": 1})
    if not user or not content or not subscription:
        raise RuntimeError("The producer requires users, content and subscriptions in Atlas")
    return user, content, subscription


def create_reproduction(user, content, subscription):
    now = datetime.now(timezone.utc)
    return {
        "codigo_evento": f"CDC{int(now.timestamp() * 1000)}",
        "usuario_id": user["_id"],
        "suscripcion_id": subscription["_id"],
        "tipo_contenido": "pelicula",
        "contenido_id": content["_id"],
        "fecha_inicio": now,
        "fecha_fin": now + timedelta(minutes=5),
        "duracion_vista_minutos": 1,
        "minuto_actual": 1,
        "dispositivo": random.choice(["Web", "Smart TV", "Movil"]),
        "pais": "Colombia",
        "calidad": random.choice(["HD", "Full HD", "4K"]),
        "estado": "En curso",
        "streaming_demo": True,
    }


def generate_once(database) -> None:
    user, content, subscription = load_reference_data(database)
    reproduction = create_reproduction(user, content, subscription)
    result = database["reproducciones"].insert_one(reproduction)
    LOGGER.info("Inserted reproduction %s", result.inserted_id)
    database["reproducciones"].update_one({"_id": result.inserted_id}, {"$set": {"minuto_actual": 3, "estado": "Pausada"}})
    LOGGER.info("Updated reproduction %s", result.inserted_id)

    temporary = database[load_streaming_settings().temporary_collection]
    temporary_id = temporary.insert_one({"streaming_demo": True, "created_at": datetime.now(timezone.utc), "source_id": result.inserted_id}).inserted_id
    temporary.delete_one({"_id": temporary_id})
    LOGGER.info("Inserted and deleted safe demo document %s in %s", temporary_id, temporary.name)


def generate_forever(database, interval: float) -> None:
    user, content, subscription = load_reference_data(database)
    while True:
        result = database["reproducciones"].insert_one(create_reproduction(user, content, subscription))
        LOGGER.info("Generated reproduction %s", result.inserted_id)
        time.sleep(interval)


def main() -> None:
    args = build_parser().parse_args()
    settings = load_streaming_settings()
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5_000, connectTimeoutMS=5_000, appname="hyperflix-cdc-producer")
    try:
        database = client[settings.database_name]
        if args.once:
            generate_once(database)
        else:
            generate_forever(database, args.interval)
    except KeyboardInterrupt:
        LOGGER.info("Producer stopped")
    finally:
        client.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    main()