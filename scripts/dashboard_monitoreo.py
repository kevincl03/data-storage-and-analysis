"""OLAP monitoring dashboard and benchmark for the Hyperflix warehouse."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


LOGGER = logging.getLogger("dashboard_olap")
WAREHOUSE_COLLECTIONS = ("dim_tiempo", "dim_usuario", "dim_contenido", "fact_visualizaciones")
SLA_MS = float(os.getenv("DASHBOARD_SLA_MS", "500"))


def load_settings() -> tuple[str, str, Path]:
    load_dotenv()
    uri = os.getenv("MONGO_URI", "").strip()
    database = os.getenv("MONGO_DB_NAME", "hyperflix_db").strip()
    output = Path(os.getenv("DASHBOARD_OUTPUT_DIR", "dashboard_output"))
    if not uri:
        raise ValueError("Missing required environment variable: MONGO_URI")
    return uri, database, output


def connect(uri: str) -> MongoClient:
    client = MongoClient(uri, serverSelectionTimeoutMS=5_000, connectTimeoutMS=5_000, appname="hyperflix-olap-dashboard")
    client.admin.command("ping")
    return client


def create_olap_indexes(database: Database) -> None:
    fact = database["fact_visualizaciones"]
    indexes = [
        ([('fecha_id', 1), ('usuario_id', 1)], "idx_fact_fecha_usuario"),
        ([('fecha_id', 1), ('contenido_id', 1)], "idx_fact_fecha_contenido"),
        ([('usuario_id', 1), ('suscripcion_id', 1)], "idx_fact_usuario_plan"),
        ([('contenido_id', 1), ('minutos_reproducidos', -1)], "idx_fact_contenido_minutos"),
    ]
    for keys, name in indexes:
        fact.create_index(keys, name=name)
    database["dim_usuario"].create_index([('ciudad', 1), ('pais', 1)], name="idx_usuario_region")
    database["dim_contenido"].create_index([('content_type', 1), ('generos', 1)], name="idx_contenido_tipo_genero")
    database["pagos"].create_index([('fecha_pago', 1), ('estado', 1), ('moneda', 1)], name="idx_pagos_fecha_estado")


def run_pipeline(collection: Collection, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return list(collection.aggregate(pipeline, allowDiskUse=True))


def enriched_fact_pipeline() -> list[dict[str, Any]]:
    return [
        {"$lookup": {"from": "dim_tiempo", "localField": "fecha_id", "foreignField": "fecha_id", "as": "tiempo"}},
        {"$unwind": "$tiempo"},
        {"$lookup": {"from": "dim_usuario", "localField": "usuario_id", "foreignField": "usuario_id", "as": "usuario"}},
        {"$unwind": {"path": "$usuario", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {"from": "dim_contenido", "localField": "contenido_id", "foreignField": "contenido_id", "as": "contenido"}},
        {"$unwind": {"path": "$contenido", "preserveNullAndEmptyArrays": True}},
    ]


def query_01_monthly_consumption_and_payments(database: Database) -> dict[str, Any]:
    consumption = run_pipeline(database["fact_visualizaciones"], enriched_fact_pipeline() + [
        {"$group": {"_id": {"anio": "$tiempo.anio", "mes": "$tiempo.mes"}, "reproducciones": {"$sum": 1}, "minutos": {"$sum": "$minutos_reproducidos"}, "usuarios_unicos": {"$addToSet": "$usuario_id"}}},
        {"$project": {"_id": 1, "reproducciones": 1, "minutos_totales": 1, "horas_totales": {"$divide": ["$minutos", 60]}, "usuarios_unicos": {"$size": "$usuarios_unicos"}}},
        {"$sort": {"_id.anio": 1, "_id.mes": 1}},
    ])
    payments = run_pipeline(database["pagos"], [
        {"$match": {"estado": "Completado"}},
        {"$project": {"anio": {"$year": "$fecha_pago"}, "mes": {"$month": "$fecha_pago"}, "monto": 1, "moneda": 1}},
        {"$group": {"_id": {"anio": "$anio", "mes": "$mes", "moneda": "$moneda"}, "pagos_completados": {"$sum": 1}, "monto_total": {"$sum": "$monto"}}},
        {"$sort": {"_id.anio": 1, "_id.mes": 1}},
    ])
    return {"consumption": consumption, "payments": payments}


def query_02_top_content_by_genre(database: Database) -> list[dict[str, Any]]:
    return run_pipeline(database["fact_visualizaciones"], enriched_fact_pipeline() + [
        {"$unwind": {"path": "$contenido.generos", "preserveNullAndEmptyArrays": True}},
        {"$group": {"_id": {"genero": {"$ifNull": ["$contenido.generos", "$contenido.categoria"]}, "contenido_id": "$contenido_id", "titulo": "$contenido.titulo"}, "reproducciones": {"$sum": 1}, "minutos": {"$sum": "$minutos_reproducidos"}, "usuarios": {"$addToSet": "$usuario_id"}}},
        {"$sort": {"_id.genero": 1, "minutos": -1}},
        {"$group": {"_id": "$_id.genero", "contenidos": {"$push": {"contenido_id": "$_id.contenido_id", "titulo": "$_id.titulo", "reproducciones": "$reproducciones", "minutos": "$minutos", "usuarios_unicos": {"$size": "$usuarios"}}}}},
        {"$project": {"genero": "$_id", "top_5": {"$slice": ["$contenidos", 5]}, "_id": 0}},
    ])


def query_03_segment_city(database: Database) -> list[dict[str, Any]]:
    return run_pipeline(database["fact_visualizaciones"], enriched_fact_pipeline() + [
        {"$group": {"_id": {"ciudad": "$usuario.ciudad", "pais": "$usuario.pais", "usuario_id": "$usuario_id"}, "minutos": {"$sum": "$minutos_reproducidos"}, "sesiones": {"$sum": 1}}},
        {"$group": {"_id": {"ciudad": "$_id.ciudad", "pais": "$_id.pais"}, "minutos_consumidos": {"$sum": "$minutos"}, "usuarios": {"$sum": 1}, "sesiones": {"$sum": "$sesiones"}}},
        {"$sort": {"minutos_consumidos": -1}},
    ])


def query_04_device_and_plan(database: Database) -> list[dict[str, Any]]:
    return run_pipeline(database["reproducciones"], [
        {"$lookup": {"from": "suscripciones", "localField": "suscripcion_id", "foreignField": "_id", "as": "suscripcion"}},
        {"$unwind": {"path": "$suscripcion", "preserveNullAndEmptyArrays": True}},
        {"$group": {"_id": {"dispositivo": "$dispositivo", "plan": "$suscripcion.plan"}, "minutos_promedio": {"$avg": "$duracion_vista_minutos"}, "reproducciones": {"$sum": 1}, "usuarios": {"$addToSet": "$usuario_id"}}},
        {"$project": {"_id": 0, "dispositivo": "$_id.dispositivo", "plan": "$_id.plan", "minutos_promedio": 1, "reproducciones": 1, "usuarios_activos": {"$size": "$usuarios"}}},
        {"$sort": {"usuarios_activos": -1}},
    ])


def query_05_weekday_trend(database: Database) -> list[dict[str, Any]]:
    return run_pipeline(database["fact_visualizaciones"], enriched_fact_pipeline() + [
        {"$group": {"_id": {"dia_semana": "$tiempo.dia_semana", "nombre": "$tiempo.nombre_dia"}, "reproducciones": {"$sum": 1}, "minutos": {"$sum": "$minutos_reproducidos"}}},
        {"$sort": {"_id.dia_semana": 1}},
    ])


def query_06_dropoff_by_plan(database: Database) -> list[dict[str, Any]]:
    return run_pipeline(database["reproducciones"], [
        {"$lookup": {"from": "suscripciones", "localField": "suscripcion_id", "foreignField": "_id", "as": "suscripcion"}},
        {"$unwind": {"path": "$suscripcion", "preserveNullAndEmptyArrays": True}},
        {"$group": {"_id": "$suscripcion.plan", "sesiones": {"$sum": 1}, "abandonadas": {"$sum": {"$cond": [{"$in": ["$estado", ["Abandonada", "Error"]]}, 1, 0]}}}},
        {"$project": {"_id": 0, "plan": "$_id", "sesiones": 1, "abandonadas": 1, "tasa_abandono": {"$multiply": [{"$divide": ["$abandonadas", {"$max": ["$sesiones", 1]}]}, 100]}}},
        {"$sort": {"tasa_abandono": -1}},
    ])


def query_07_top_vip_users(database: Database) -> list[dict[str, Any]]:
    return run_pipeline(database["fact_visualizaciones"], [
        {"$group": {"_id": "$usuario_id", "minutos": {"$sum": "$minutos_reproducidos"}, "reproducciones": {"$sum": 1}}},
        {"$lookup": {"from": "dim_usuario", "localField": "_id", "foreignField": "usuario_id", "as": "usuario"}},
        {"$unwind": "$usuario"},
        {"$project": {"_id": 0, "usuario_id": "$_id", "nombre": "$usuario.nombre", "ciudad": "$usuario.ciudad", "horas_consumidas": {"$divide": ["$minutos", 60]}, "reproducciones": 1, "total_pagado": "$usuario.total_pagado"}},
        {"$sort": {"horas_consumidas": -1, "total_pagado": -1}},
        {"$limit": 10},
    ])


def query_08_quarterly_trend(database: Database) -> list[dict[str, Any]]:
    return run_pipeline(database["fact_visualizaciones"], enriched_fact_pipeline() + [
        {"$group": {"_id": {"anio": "$tiempo.anio", "trimestre": "$tiempo.trimestre"}, "reproducciones": {"$sum": 1}, "minutos": {"$sum": "$minutos_reproducidos"}, "usuarios": {"$addToSet": "$usuario_id"}}},
        {"$project": {"_id": 1, "reproducciones": 1, "horas_reproducidas": {"$divide": ["$minutos", 60]}, "usuarios_unicos": {"$size": "$usuarios"}}},
        {"$sort": {"_id.anio": 1, "_id.trimestre": 1}},
    ])


def query_09_retention_by_region(database: Database) -> list[dict[str, Any]]:
    return run_pipeline(database["fact_visualizaciones"], [
        {"$group": {"_id": "$usuario_id", "minutos": {"$sum": "$minutos_reproducidos"}, "sesiones": {"$sum": 1}}},
        {"$lookup": {"from": "dim_usuario", "localField": "_id", "foreignField": "usuario_id", "as": "usuario"}},
        {"$unwind": "$usuario"},
        {"$set": {"horas": {"$divide": ["$minutos", 60]}, "segmento": {"$switch": {"branches": [{"case": {"$gt": [{"$divide": ["$minutos", 60]}, 80]}, "then": "VIP Binge Watcher"}, {"case": {"$gt": [{"$divide": ["$minutos", 60]}, 25]}, "then": "Streamer Frecuente"}, {"case": {"$gt": [{"$divide": ["$minutos", 60]}, 5]}, "then": "Casual Streamer"}], "default": "Usuario Ocasional"}}}},
        {"$group": {"_id": {"ciudad": "$usuario.ciudad", "pais": "$usuario.pais", "segmento": "$segmento"}, "minutos_promedio": {"$avg": {"$divide": ["$minutos", "$sesiones"]}}, "usuarios": {"$sum": 1}}},
        {"$sort": {"minutos_promedio": -1}},
        {"$limit": 15},
    ])


def query_10_viral_content(database: Database) -> list[dict[str, Any]]:
    return run_pipeline(database["fact_visualizaciones"], [
        {"$group": {"_id": "$contenido_id", "espectadores_unicos": {"$addToSet": "$usuario_id"}, "reproducciones": {"$sum": 1}}},
        {"$lookup": {"from": "dim_contenido", "localField": "_id", "foreignField": "contenido_id", "as": "contenido"}},
        {"$unwind": {"path": "$contenido", "preserveNullAndEmptyArrays": True}},
        {"$project": {"_id": 0, "contenido_id": "$_id", "titulo": "$contenido.titulo", "tipo": "$contenido.content_type", "espectadores_unicos": {"$size": "$espectadores_unicos"}, "reproducciones": 1}},
        {"$sort": {"espectadores_unicos": -1, "reproducciones": -1}},
        {"$limit": 10},
    ])


QUERIES: list[tuple[str, Callable[[Database], Any]]] = [
    ("Consumo mensual y pagos de suscripción", query_01_monthly_consumption_and_payments),
    ("Top 5 contenidos por género", query_02_top_content_by_genre),
    ("Consumo por ciudad y usuario", query_03_segment_city),
    ("Rendimiento por dispositivo y plan", query_04_device_and_plan),
    ("Tendencia por día de la semana", query_05_weekday_trend),
    ("Tasa de abandono por plan", query_06_dropoff_by_plan),
    ("Top 10 usuarios VIP", query_07_top_vip_users),
    ("Tendencia trimestral de consumo", query_08_quarterly_trend),
    ("Retención por región y segmento", query_09_retention_by_region),
    ("Top 10 contenidos virales", query_10_viral_content),
]


def execute_benchmark(database: Database) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    results: list[dict[str, Any]] = []
    timings: list[dict[str, Any]] = []
    print("Iniciando Benchmarking de 10 Consultas Analíticas OLAP para Hyperflix...")
    print("Aplicando índices compuestos y allowDiskUse=True...")
    create_olap_indexes(database)
    print("Índices analíticos verificados.")
    for index, (name, query) in enumerate(QUERIES, start=1):
        started = time.perf_counter()
        result = query(database)
        elapsed_ms = (time.perf_counter() - started) * 1000
        rows = result["consumption"] if isinstance(result, dict) and "consumption" in result else result
        timings.append({"query": index, "name": name, "latency_ms": round(elapsed_ms, 2), "rows": len(rows)})
        results.append({"query": index, "name": name, "data": result})
        print(f"[{index:02d}/10] {name}: {elapsed_ms:.2f} ms | filas={len(rows)}")
    return results, timings


def collect_storage_metrics(database: Database) -> dict[str, Any]:
    stats = {}
    for collection_name in WAREHOUSE_COLLECTIONS:
        stats[collection_name] = database.command("collStats", collection_name)
    total_docs = sum(database[name].count_documents({}) for name in WAREHOUSE_COLLECTIONS)
    total_bytes = sum(stats[name].get("storageSize", 0) for name in WAREHOUSE_COLLECTIONS)
    return {"collections": stats, "total_docs": total_docs, "total_bytes": total_bytes, "total_gb": total_bytes / (1024 ** 3)}


def _monthly_frame(monthly: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for row in monthly:
        key = row.get("_id", {})
        rows.append({"period": f"{key.get('anio')}-{int(key.get('mes', 0)):02d}", "reproducciones": row.get("reproducciones", 0), "horas": row.get("horas_totales", 0)})
    return pd.DataFrame(rows)


def generate_dashboard(results: list[dict[str, Any]], timings: list[dict[str, Any]], storage: dict[str, Any], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    monthly_data = results[0]["data"]["consumption"]
    monthly = _monthly_frame(monthly_data)
    timing_frame = pd.DataFrame(timings)
    collection_names = list(storage["collections"])
    collection_docs = [storage["collections"][name].get("count", 0) for name in collection_names]
    atlas_cost = float(os.getenv("DASHBOARD_ATLAS_MONTHLY_COST", "0"))
    operations_cost = float(os.getenv("DASHBOARD_OPERATIONS_MONTHLY_COST", "0"))
    status = "SALUDABLE" if timing_frame["latency_ms"].mean() < SLA_MS else "REVISAR LATENCIA"

    plt.style.use("dark_background")
    figure, axes = plt.subplots(2, 3, figsize=(18, 10), facecolor="#101820")
    figure.suptitle("HYPERFLIX / OLAP CONTROL ROOM", fontsize=22, fontweight="bold", color="#F2F4F3")
    accent = "#F7B32B"
    cyan = "#4FD1C5"
    muted = "#AAB7B8"

    axes[0, 0].bar([name.replace("fact_", "").replace("dim_", "") for name in collection_names], collection_docs, color=cyan)
    axes[0, 0].set_title("Documentos por colección")
    axes[0, 0].tick_params(axis="x", rotation=35, labelsize=8)

    axes[0, 1].bar(timing_frame["query"], timing_frame["latency_ms"], color=[accent if value < SLA_MS else "#E76F51" for value in timing_frame["latency_ms"]])
    axes[0, 1].axhline(SLA_MS, color="#E76F51", linestyle="--", label=f"SLA {SLA_MS:.0f} ms")
    axes[0, 1].set_title("Latencia por consulta")
    axes[0, 1].set_xlabel("Consulta")
    axes[0, 1].set_ylabel("ms")
    axes[0, 1].legend(fontsize=8)

    if atlas_cost + operations_cost > 0:
        axes[0, 2].pie([atlas_cost, operations_cost], labels=["Atlas", "Operación"], autopct="%1.0f%%", colors=[accent, cyan], wedgeprops={"width": 0.42, "edgecolor": "#101820"})
    else:
        axes[0, 2].text(0.5, 0.5, "No configurado", ha="center", va="center", fontsize=16, color=muted)
    axes[0, 2].set_title("Estimación mensual configurada")

    if not monthly.empty:
        axes[1, 0].plot(monthly["period"], monthly["reproducciones"], marker="o", color=cyan, label="Reproducciones")
        axes[1, 0].plot(monthly["period"], monthly["horas"], marker="o", color=accent, label="Horas vistas")
        axes[1, 0].tick_params(axis="x", rotation=45, labelsize=8)
        axes[1, 0].legend(fontsize=8)
    axes[1, 0].set_title("Tendencia mensual de consumo")

    axes[1, 1].bar(["Documentos", "GB estimados"], [storage["total_docs"], storage["total_gb"]], color=[cyan, accent])
    axes[1, 1].set_title("Huella del warehouse")

    axes[1, 2].axis("off")
    summary = "\n".join([
        "RESUMEN EJECUTIVO",
        f"Estado: {status}",
        f"Consultas: {len(timings)}",
        f"Latencia media: {timing_frame['latency_ms'].mean():.1f} ms",
        f"Latencia máxima: {timing_frame['latency_ms'].max():.1f} ms",
        f"Documentos: {storage['total_docs']:,}",
        f"Colecciones activas: {len(collection_names)}",
        "\nPagos reportados como suscripciones;\nno atribuidos a reproducciones.",
    ])
    axes[1, 2].text(0.04, 0.95, summary, va="top", fontsize=12, color=muted, linespacing=1.6)
    figure.tight_layout(rect=[0, 0, 1, 0.95])
    output_path = output_dir / f"dashboard_monitoreo_{datetime.now().strftime('%Y%m%d')}.png"
    figure.savefig(output_path, dpi=160, facecolor=figure.get_facecolor(), bbox_inches="tight")
    plt.close(figure)
    return output_path


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    uri, database_name, output_dir = load_settings()
    client = connect(uri)
    try:
        database = client[database_name]
        results, timings = execute_benchmark(database)
        storage = collect_storage_metrics(database)
        output_path = generate_dashboard(results, timings, storage, output_dir)
        average = sum(item["latency_ms"] for item in timings) / len(timings)
        maximum = max(timings, key=lambda item: item["latency_ms"])
        print("=" * 72)
        print("RESUMEN DE BENCHMARKING OLAP HYPERFLIX")
        print(f"Consultas ejecutadas: {len(timings)}")
        print(f"Latencia promedio: {average:.2f} ms")
        print(f"Latencia máxima: {maximum['latency_ms']:.2f} ms ({maximum['name']})")
        print(f"Documentos warehouse: {storage['total_docs']:,}")
        print(f"Dashboard generado: {output_path}")
    finally:
        client.close()


if __name__ == "__main__":
    main()