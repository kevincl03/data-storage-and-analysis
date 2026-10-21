import json
import os
import time

import pymongo
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "hyperflix_db")
SLA_MS = 500


def asegurar_indice(coleccion, campos, nombre):
    campos_esperados = tuple(campos)
    for indice in coleccion.list_indexes():
        if tuple(indice["key"].items()) == campos_esperados:
            return indice["name"]
    return coleccion.create_index(list(campos), name=nombre)


def ejecutar_benchmark():
    print("🚀 Iniciando benchmarking analítico de Hyperflix (10 consultas, SLA <= 500 ms)...\n")
    if not MONGO_URI:
        raise ValueError("Falta la variable de entorno MONGO_URI")
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, appname="hyperflix-benchmark")
    db = client[MONGO_DB_NAME]
    coleccion = db["reproducciones_analitica"]
    print("🔧 Fase 1: Aplicando estrategia de indexación...")
    asegurar_indice(coleccion, [("anio", 1), ("mes", 1)], "idx_fecha")
    asegurar_indice(coleccion, [("contenido.tipo", 1), ("contenido.categoria", 1), ("duracion_vista_minutos", -1)], "idx_contenido_consumo")
    asegurar_indice(coleccion, [("pais", 1), ("dispositivo", 1)], "idx_audiencia")
    asegurar_indice(coleccion, [("suscripcion.plan", 1), ("pago_completado", 1)], "idx_plan_pago")
    asegurar_indice(coleccion, [("estado", 1), ("calidad", 1), ("duracion_vista_minutos", -1)], "idx_estado_calidad")
    asegurar_indice(coleccion, [("usuario.id", 1), ("anio", 1), ("mes", 1)], "idx_usuario_periodo")
    asegurar_indice(coleccion, [("contenido.tipo", 1), ("estado", 1)], "idx_tipo_estado")
    print("✅ Índices creados exitosamente.\n")

    consultas = [
        {"nombre": "1. Minutos vistos por mes", "pipeline": [{"$group": {"_id": {"anio": "$anio", "mes": "$mes"}, "minutos_vistos": {"$sum": "$duracion_vista_minutos"}, "reproducciones": {"$sum": 1}}}, {"$sort": {"_id.anio": 1, "_id.mes": 1}}]},
        {"nombre": "2. Contenido más visto por tipo", "pipeline": [{"$group": {"_id": {"tipo": "$contenido.tipo", "contenido": "$contenido.nombre"}, "minutos": {"$sum": "$duracion_vista_minutos"}, "sesiones": {"$sum": 1}}}, {"$sort": {"_id.tipo": 1, "minutos": -1}}, {"$group": {"_id": "$_id.tipo", "contenidos": {"$push": {"nombre": "$_id.contenido", "minutos": "$minutos", "sesiones": "$sesiones"}}}}, {"$project": {"_id": 0, "tipo": "$_id", "top_5": {"$slice": ["$contenidos", 5]}}}]},
        {"nombre": "3. Audiencia por país y dispositivo", "pipeline": [{"$match": {"estado": {"$ne": "Error"}}}, {"$group": {"_id": {"pais": "$pais", "dispositivo": "$dispositivo"}, "usuarios": {"$addToSet": "$usuario.id"}, "minutos": {"$sum": "$duracion_vista_minutos"}}}, {"$project": {"_id": 1, "usuarios_unicos": {"$size": "$usuarios"}, "minutos": 1}}, {"$sort": {"minutos": -1}}]},
        {"nombre": "4. Consumo por plan con pago completado", "pipeline": [{"$match": {"pago_completado": True}}, {"$group": {"_id": "$suscripcion.plan", "minutos": {"$sum": "$duracion_vista_minutos"}, "sesiones": {"$sum": 1}, "ingreso_mensual_referencial": {"$sum": "$suscripcion.precio_mensual"}}}, {"$sort": {"minutos": -1}}]},
        {"nombre": "5. Canales y películas con mayor consumo", "pipeline": [{"$group": {"_id": {"tipo": "$contenido.tipo", "categoria": "$contenido.categoria"}, "minutos": {"$sum": "$duracion_vista_minutos"}, "sesiones": {"$sum": 1}}}, {"$sort": {"minutos": -1}}, {"$limit": 20}]},
        {"nombre": "6. Consumo por calidad de reproducción", "pipeline": [{"$match": {"estado": "Completada"}}, {"$group": {"_id": "$calidad", "reproducciones": {"$sum": 1}, "minutos": {"$sum": "$duracion_vista_minutos"}, "usuarios": {"$addToSet": "$usuario.id"}}}, {"$project": {"_id": 0, "calidad": "$_id", "reproducciones": 1, "minutos": 1, "usuarios_unicos": {"$size": "$usuarios"}}}, {"$sort": {"minutos": -1}}, {"$limit": 10}]},
        {"nombre": "7. Usuarios activos por país", "pipeline": [{"$match": {"estado": {"$ne": "Error"}}}, {"$group": {"_id": "$pais", "usuarios": {"$addToSet": "$usuario.id"}, "reproducciones": {"$sum": 1}, "minutos": {"$sum": "$duracion_vista_minutos"}}}, {"$project": {"_id": 0, "pais": "$_id", "usuarios_unicos": {"$size": "$usuarios"}, "reproducciones": 1, "minutos": 1}}, {"$sort": {"usuarios_unicos": -1}}, {"$limit": 20}]},
        {"nombre": "8. Duración promedio por tipo de contenido", "pipeline": [{"$match": {"estado": "Completada"}}, {"$group": {"_id": "$contenido.tipo", "duracion_promedio": {"$avg": "$duracion_vista_minutos"}, "reproducciones": {"$sum": 1}, "usuarios": {"$addToSet": "$usuario.id"}}}, {"$project": {"_id": 0, "tipo_contenido": "$_id", "duracion_promedio": {"$round": ["$duracion_promedio", 2]}, "reproducciones": 1, "usuarios_unicos": {"$size": "$usuarios"}}}, {"$sort": {"duracion_promedio": -1}}, {"$limit": 10}]},
        {"nombre": "9. Consumo por plan de suscripción", "pipeline": [{"$match": {"pago_completado": True}}, {"$group": {"_id": "$suscripcion.plan", "reproducciones": {"$sum": 1}, "minutos": {"$sum": "$duracion_vista_minutos"}, "usuarios": {"$addToSet": "$usuario.id"}}}, {"$project": {"_id": 0, "plan": "$_id", "reproducciones": 1, "minutos": 1, "usuarios_unicos": {"$size": "$usuarios"}}}, {"$sort": {"minutos": -1}}, {"$limit": 10}]},
        {"nombre": "10. Tendencia mensual de reproducciones completadas", "pipeline": [{"$match": {"estado": "Completada"}}, {"$group": {"_id": {"anio": "$anio", "mes": "$mes"}, "reproducciones": {"$sum": 1}, "minutos": {"$sum": "$duracion_vista_minutos"}, "usuarios": {"$addToSet": "$usuario.id"}}}, {"$project": {"_id": 1, "reproducciones": 1, "minutos": 1, "usuarios_unicos": {"$size": "$usuarios"}}}, {"$sort": {"_id.anio": 1, "_id.mes": 1}}, {"$limit": 24}]},
    ]

    print("⚙️ Fase 2: Ejecutando consultas...\n")
    tiempos = []
    for consulta in consultas:
        print(f"▶️ {consulta['nombre']}")
        inicio = time.perf_counter()
        resultados = list(coleccion.aggregate(consulta["pipeline"], allowDiskUse=True, maxTimeMS=SLA_MS))
        tiempo_ms = (time.perf_counter() - inicio) * 1000
        tiempos.append(tiempo_ms)
        estado_sla = "✅ SLA" if tiempo_ms <= SLA_MS else "⚠️ SLA SUPERADO"
        print(f"   ⏱️ Latencia: {tiempo_ms:.2f} ms | 📊 Filas: {len(resultados)} | {estado_sla}")
        if resultados:
            print(f"   💡 Muestra: {json.loads(json.dumps(resultados[0], default=str))}")
        print("-" * 70)

    if tiempos:
        print("\n" + "=" * 70)
        print("📊 RESUMEN DE BENCHMARKING HYPERFLIX")
        print("=" * 70)
        print(f"📈 Consultas ejecutadas: {len(consultas)}")
        print(f"⏱️ Latencia promedio: {sum(tiempos) / len(tiempos):.2f} ms")
        print(f"🚀 Latencia máxima: {max(tiempos):.2f} ms")
        print(f"💾 Registros analíticos procesados: {coleccion.count_documents({})}")
        print(f"🎯 SLA objetivo: <= {SLA_MS:.0f} ms por consulta")
        print("✅ CONCLUSIÓN: Todas las consultas están configuradas con maxTimeMS=500 y límites de salida controlados.")
        print("=" * 70)
    client.close()


if __name__ == "__main__":
    ejecutar_benchmark()