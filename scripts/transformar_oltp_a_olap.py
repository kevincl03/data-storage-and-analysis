import os
import time

import pymongo
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")


def transformar_datos():
    print("🔄 Iniciando transformación OLTP a OLAP de Hyperflix...\n")
    client = pymongo.MongoClient(MONGO_URI)
    db = client["hyperflix_db"]
    inicio = time.time()
    pipeline = [
        {"$lookup": {"from": "usuarios", "localField": "usuario_id", "foreignField": "_id", "as": "usuario"}},
        {"$unwind": {"path": "$usuario", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {"from": "peliculas", "localField": "contenido_id", "foreignField": "_id", "as": "pelicula"}},
        {"$unwind": {"path": "$pelicula", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {"from": "canales_tv", "localField": "contenido_id", "foreignField": "_id", "as": "canal"}},
        {"$unwind": {"path": "$canal", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {"from": "suscripciones", "localField": "suscripcion_id", "foreignField": "_id", "as": "suscripcion"}},
        {"$unwind": {"path": "$suscripcion", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {"from": "pagos", "let": {"usuario": "$usuario_id", "suscripcion": "$suscripcion_id"}, "pipeline": [{"$match": {"$expr": {"$and": [{"$eq": ["$usuario_id", "$$usuario"]}, {"$eq": ["$suscripcion_id", "$$suscripcion"]}, {"$eq": ["$estado", "Completado"]}]}}}, {"$sort": {"fecha_pago": -1}}, {"$limit": 1}], "as": "pago"}},
        {"$unwind": {"path": "$pago", "preserveNullAndEmptyArrays": True}},
        {"$set": {"nombre_contenido": {"$ifNull": ["$pelicula.titulo", "$canal.nombre"]}, "categoria_contenido": {"$ifNull": [{"$arrayElemAt": ["$pelicula.generos", 0]}, "$canal.categoria"]}}},
        {"$project": {
            "_id": 1, "codigo_evento": 1, "fecha_inicio": 1, "fecha_fin": 1,
            "anio": {"$year": "$fecha_inicio"}, "mes": {"$month": "$fecha_inicio"}, "dia": {"$dayOfMonth": "$fecha_inicio"}, "dia_semana": {"$dayOfWeek": "$fecha_inicio"},
            "usuario": {"id": "$usuario._id", "nombre": "$usuario.nombre", "pais": "$usuario.pais"},
            "contenido": {"id": "$contenido_id", "tipo": "$tipo_contenido", "nombre": "$nombre_contenido", "categoria": "$categoria_contenido"},
            "suscripcion": {"id": "$suscripcion._id", "plan": "$suscripcion.plan", "precio_mensual": "$suscripcion.precio_mensual"},
            "pago_completado": {"$ne": [{"$type": "$pago._id"}, "missing"]},
            "duracion_vista_minutos": 1, "dispositivo": 1, "pais": 1, "calidad": 1, "estado": 1,
        }},
        {"$merge": {"into": "reproducciones_analitica", "on": "_id", "whenMatched": "replace", "whenNotMatched": "insert"}},
    ]
    print("⚙️ Ejecutando pipeline de enriquecimiento...")
    list(db.reproducciones.aggregate(pipeline, allowDiskUse=True))
    print("=" * 60)
    print("🎉 ¡TRANSFORMACIÓN OLTP A OLAP COMPLETADA!")
    print(f"⏱️ Tiempo de procesamiento: {time.time() - inicio:.2f} segundos")
    print(f"📊 Registros analíticos generados: {db.reproducciones_analitica.count_documents({})}")
    print("=" * 60)
    client.close()


if __name__ == "__main__":
    transformar_datos()