"""Read-only MongoDB to local three-zone Data Lake pipeline."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


LOGGER = logging.getLogger("data_lake")
DEFAULT_COLLECTIONS = {
    "users": "usuarios",
    "content": "peliculas,canales_tv",
    "subscriptions": "suscripciones",
    "payments": "pagos",
    "events": "reproducciones",
}
DEFAULT_COLLECTION_ENV = {
    "MONGO_USER_COLLECTION": "users",
    "MONGO_CONTENT_COLLECTIONS": "content",
    "MONGO_SUBSCRIPTION_COLLECTION": "subscriptions",
    "MONGO_PAYMENT_COLLECTION": "payments",
    "MONGO_EVENT_COLLECTION": "events",
}


@dataclass(frozen=True)
class Settings:
    mongo_uri: str
    mongo_db_name: str
    data_lake_root: Path
    collection_names: dict[str, list[str]]
    batch_size: int = 1_000


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _collection_list(name: str) -> list[str]:
    default_role = DEFAULT_COLLECTION_ENV[name]
    raw_value = os.getenv(name, DEFAULT_COLLECTIONS[default_role])
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def load_settings() -> Settings:
    load_dotenv()
    batch_size = int(os.getenv("MONGO_BATCH_SIZE", "1000"))
    if batch_size <= 0:
        raise ValueError("MONGO_BATCH_SIZE must be greater than zero")

    return Settings(
        mongo_uri=_required_env("MONGO_URI"),
        mongo_db_name=os.getenv("MONGO_DB_NAME", "hyperflix_db").strip(),
        data_lake_root=Path(os.getenv("DATA_LAKE_ROOT", "data_lake")),
        collection_names={
            "users": _collection_list("MONGO_USER_COLLECTION"),
            "content": _collection_list("MONGO_CONTENT_COLLECTIONS"),
            "subscriptions": _collection_list("MONGO_SUBSCRIPTION_COLLECTION"),
            "payments": _collection_list("MONGO_PAYMENT_COLLECTION"),
            "events": _collection_list("MONGO_EVENT_COLLECTION"),
        },
        batch_size=batch_size,
    )


def create_lake_structure(root: Path) -> dict[str, Path]:
    zones = {zone: root / zone for zone in ("raw", "processed", "curated")}
    for path in zones.values():
        path.mkdir(parents=True, exist_ok=True)
    return zones


def get_mongo_client(settings: Settings) -> MongoClient:
    try:
        client = MongoClient(
            settings.mongo_uri,
            serverSelectionTimeoutMS=5_000,
            connectTimeoutMS=5_000,
            appname="hyperflix-data-lake",
        )
        client.admin.command("ping")
        return client
    except PyMongoError as error:
        raise ConnectionError("Could not connect to MongoDB Atlas") from error


def _serialize_bson(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value
    if isinstance(value, dict):
        return {str(key): _serialize_bson(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_bson(item) for item in value]
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return value


def _json_safe(value: Any) -> str:
    return json.dumps(_serialize_bson(value), ensure_ascii=False, default=str)


def documents_to_frame(documents: Iterable[dict[str, Any]]) -> pd.DataFrame:
    normalized = [_serialize_bson(document) for document in documents]
    if not normalized:
        return pd.DataFrame()
    frame = pd.json_normalize(normalized, sep=".")
    for column in frame.columns:
        if frame[column].map(lambda value: isinstance(value, (dict, list))).any():
            frame[column] = frame[column].map(_json_safe)
    return frame


def extract_collection(
    database: Any,
    collection_name: str,
    output_path: Path,
    batch_size: int,
) -> pd.DataFrame:
    collection: Collection = database[collection_name]
    documents: list[dict[str, Any]] = []
    cursor = collection.find({}, batch_size=batch_size)
    for document in cursor:
        documents.append(document)
    frame = documents_to_frame(documents)
    write_parquet(frame, output_path)
    LOGGER.info("RAW %-20s rows=%s path=%s", collection_name, len(frame), output_path)
    return frame


def write_parquet(frame: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)


def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path, engine="pyarrow")


def _first_existing(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    return next((column for column in candidates if column in frame.columns), None)


def _string_series(frame: pd.DataFrame, column: str | None) -> pd.Series:
    if column is None:
        return pd.Series(pd.NA, index=frame.index, dtype="string")
    return frame[column].astype("string")


def _date_frame(frame: pd.DataFrame, candidates: list[str]) -> tuple[pd.DataFrame, str | None]:
    date_column = _first_existing(frame, candidates)
    if date_column is None:
        return frame, None
    frame = frame.copy()
    parsed = pd.to_datetime(frame[date_column], errors="coerce", utc=True)
    frame[date_column] = parsed
    frame["anio"] = parsed.dt.year.astype("Int64")
    frame["mes"] = parsed.dt.month.astype("Int64")
    frame["dia"] = parsed.dt.day.astype("Int64")
    frame["fecha_procesamiento"] = datetime.now(timezone.utc)
    frame["fecha_invalida"] = frame[date_column].isna()
    return frame, date_column


def process_events(raw_path: Path, processed_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = read_parquet(raw_path)
    input_count = len(frame)
    frame, date_column = _date_frame(frame, ["fecha_inicio", "fecha", "fecha_evento", "created_at"])
    invalid_dates_input = int(frame.get("fecha_invalida", pd.Series(dtype=bool)).sum())
    status_column = _first_existing(frame, ["estado", "status"])
    if status_column:
        completed = frame[status_column].astype("string").str.casefold().eq("completada")
        frame = frame.loc[completed].copy()
    frame["event_id"] = _string_series(frame, _first_existing(frame, ["_id", "codigo_evento"]))
    frame["user_id"] = _string_series(frame, _first_existing(frame, ["usuario_id", "user_id"]))
    frame["content_id"] = _string_series(frame, _first_existing(frame, ["contenido_id", "content_id"]))
    write_parquet(frame, processed_path)
    return frame, {
        "input_rows": input_count,
        "output_rows": len(frame),
        "date_column": date_column,
        "invalid_dates_input": invalid_dates_input,
        "status_column": status_column,
    }


def _read_first_available(raw_paths: dict[str, list[Path]], role: str) -> pd.DataFrame:
    frames = [read_parquet(path) for path in raw_paths.get(role, []) if path.exists()]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build_curated(
    raw_paths: dict[str, list[Path]],
    processed_events: pd.DataFrame,
    curated_zone: Path,
) -> dict[str, pd.DataFrame]:
    users = _read_first_available(raw_paths, "users")
    content_frames = []
    for path in raw_paths.get("content", []):
        if path.exists():
            content_frame = read_parquet(path)
            content_frame["content_type"] = path.stem
            content_frames.append(content_frame)
    content = pd.concat(content_frames, ignore_index=True) if content_frames else pd.DataFrame()
    subscriptions = _read_first_available(raw_paths, "subscriptions")

    dimensions: dict[str, pd.DataFrame] = {}
    if not users.empty:
        users = users.copy()
        users["user_id"] = _string_series(users, _first_existing(users, ["_id", "codigo_usuario"]))
        dimensions["dim_usuario"] = users
    if not content.empty:
        content = content.copy()
        content["content_id"] = _string_series(content, _first_existing(content, ["_id", "codigo_pelicula", "codigo_canal"]))
        dimensions["dim_contenido"] = content
    if not subscriptions.empty:
        subscriptions = subscriptions.copy()
        subscriptions["subscription_id"] = _string_series(subscriptions, _first_existing(subscriptions, ["_id", "codigo_suscripcion"]))
        dimensions["dim_suscripcion"] = subscriptions

    facts = processed_events.copy()
    if not facts.empty:
        date_column = _first_existing(facts, ["fecha_inicio", "fecha", "fecha_evento", "created_at"])
        facts["date_id"] = facts[date_column].dt.strftime("%Y%m%d") if date_column else pd.NA
        facts["duration_minutes"] = pd.to_numeric(facts.get("duracion_vista_minutos"), errors="coerce")
        facts["fact_id"] = facts["event_id"]
    dimensions["fact_reproducciones"] = facts

    if facts.empty or "date_id" not in facts.columns:
        dimensions["dim_fecha"] = pd.DataFrame(columns=["date_id", "fecha", "dia", "mes", "anio"])
    else:
        date_column = _first_existing(facts, ["fecha_inicio", "fecha", "fecha_evento", "created_at"])
        dates = facts[["date_id", date_column, "dia", "mes", "anio"]].drop_duplicates("date_id")
        dates = dates.rename(columns={date_column: "fecha"})
        dates["trimestre"] = ((dates["mes"] - 1) // 3 + 1).astype("Int64")
        dimensions["dim_fecha"] = dates

    for name, frame in dimensions.items():
        write_parquet(frame, curated_zone / f"{name}.parquet")

    analytical = facts.copy()
    if not analytical.empty and not users.empty and "user_id" in users.columns:
        user_columns = [column for column in ["user_id", "nombre", "pais", "ciudad"] if column in users.columns]
        analytical = analytical.merge(users[user_columns], on="user_id", how="left", suffixes=("", "_usuario"))
    if not analytical.empty and not content.empty and "content_id" in content.columns:
        content_columns = [column for column in ["content_id", "content_type", "titulo", "nombre", "generos", "categoria"] if column in content.columns]
        analytical = analytical.merge(content[content_columns], on="content_id", how="left", suffixes=("", "_contenido"))
    write_parquet(analytical, curated_zone / "consumo_analitico.parquet")
    return dimensions | {"consumo_analitico": analytical}


def validate_curated(
    processed_events: pd.DataFrame,
    curated: dict[str, pd.DataFrame],
) -> dict[str, int]:
    facts = curated.get("fact_reproducciones", pd.DataFrame())
    users = curated.get("dim_usuario", pd.DataFrame())
    content = curated.get("dim_contenido", pd.DataFrame())
    event_ids = facts.get("event_id", pd.Series(dtype="string")).dropna()
    user_ids = set(users.get("user_id", pd.Series(dtype="string")).dropna())
    content_ids = set(content.get("content_id", pd.Series(dtype="string")).dropna())
    fact_user_ids = set(facts.get("user_id", pd.Series(dtype="string")).dropna())
    fact_content_ids = set(facts.get("content_id", pd.Series(dtype="string")).dropna())
    result = {
        "processed_rows": len(processed_events),
        "fact_rows": len(facts),
        "duplicate_event_ids": int(event_ids.duplicated().sum()),
        "null_event_ids": int(facts.get("event_id", pd.Series(dtype="string")).isna().sum()),
        "orphan_user_ids": len(fact_user_ids - user_ids),
        "orphan_content_ids": len(fact_content_ids - content_ids),
    }
    LOGGER.info(
        "Quality processed=%s facts=%s duplicates=%s null_keys=%s orphan_users=%s orphan_content=%s",
        result["processed_rows"],
        result["fact_rows"],
        result["duplicate_event_ids"],
        result["null_event_ids"],
        result["orphan_user_ids"],
        result["orphan_content_ids"],
    )
    return result


def summarize_lake(zones: dict[str, Path]) -> dict[str, Any]:
    summary: dict[str, Any] = {"zones": {}, "files": 0, "bytes": 0}
    for zone, path in zones.items():
        files = list(path.rglob("*.parquet"))
        size = sum(file.stat().st_size for file in files)
        summary["zones"][zone] = {"files": len(files), "bytes": size}
        summary["files"] += len(files)
        summary["bytes"] += size
        LOGGER.info("%s files=%s size=%s bytes", zone.upper(), len(files), size)
    LOGGER.info("TOTAL files=%s size=%s bytes", summary["files"], summary["bytes"])
    return summary


def _role_paths(zones: dict[str, Path], settings: Settings) -> dict[str, list[Path]]:
    paths: dict[str, list[Path]] = {}
    for role, collections in settings.collection_names.items():
        paths[role] = [zones["raw"] / f"{collection}.parquet" for collection in collections]
    return paths


def generate_complete_data_lake() -> dict[str, Any]:
    settings = load_settings()
    zones = create_lake_structure(settings.data_lake_root)
    client = get_mongo_client(settings)
    raw_paths = _role_paths(zones, settings)
    try:
        database = client[settings.mongo_db_name]
        available = set(database.list_collection_names())
        configured = [name for names in settings.collection_names.values() for name in names]
        missing = sorted(set(configured) - available)
        if missing:
            raise ValueError(f"Configured MongoDB collections do not exist: {', '.join(missing)}")

        LOGGER.info("Extracting RAW data from database '%s'", settings.mongo_db_name)
        for collection_name in configured:
            extract_collection(database, collection_name, zones["raw"] / f"{collection_name}.parquet", settings.batch_size)

        event_paths = raw_paths["events"]
        if not event_paths:
            raise ValueError("At least one event collection is required")
        processed_events, processing_stats = process_events(event_paths[0], zones["processed"] / "reproducciones_processed.parquet")
        curated = build_curated(raw_paths, processed_events, zones["curated"])
        quality_stats = validate_curated(processed_events, curated)
        summary = summarize_lake(zones)
        return {"processing": processing_stats, "quality": quality_stats, "curated": curated, "summary": summary}
    finally:
        client.close()


def main() -> None:
    configure_logging()
    try:
        generate_complete_data_lake()
    except (ConnectionError, ValueError, OSError, PyMongoError, ImportError) as error:
        LOGGER.error("Data Lake pipeline failed: %s", error)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()