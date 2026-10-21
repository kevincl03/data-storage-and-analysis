import os
import random
import time
from datetime import datetime, timedelta

import pymongo
from dotenv import load_dotenv
from faker import Faker

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
fake = Faker("es_CO")

NUM_USUARIOS = 5000
NUM_PELICULAS = 1000
NUM_CANALES = 100
NUM_SUSCRIPCIONES = 8000
NUM_PAGOS = 12000
NUM_REPRODUCCIONES = 50000
BATCH_SIZE = 1000


def conectar_bd():
    client = pymongo.MongoClient(MONGO_URI)
    return client, client["hyperflix_db"]


def insertar_en_lotes(coleccion, datos, nombre_entidad):
    total = len(datos)
    print(f"📦 Insertando {total} {nombre_entidad} en lotes de {BATCH_SIZE}...")
    for inicio in range(0, total, BATCH_SIZE):
        lote = datos[inicio:inicio + BATCH_SIZE]
        try:
            coleccion.insert_many(lote, ordered=False)
        except pymongo.errors.BulkWriteError as error:
            insertados = error.details.get("nInserted", 0)
            print(f"  ⚠️ Se omitieron {len(lote) - insertados} registros existentes")
        progreso = min(inicio + BATCH_SIZE, total)
        print(f"  ➡️ Progreso: {progreso}/{total} ({progreso / total * 100:.1f}%)")
    print(f"  ✅ Proceso de {nombre_entidad} finalizado.\n")


def generar_datos():
    print("🚀 Iniciando generación de datos masivos para Hyperflix...\n")
    client, db = conectar_bd()
    inicio_total = time.time()
    ahora = datetime.now()

    print("👤 Generando usuarios...")
    usuarios = [
        {
            "codigo_usuario": f"USR{i:06d}", "nombre": fake.name(),
            "email": f"hyperflix.user{i:06d}@example.com",
            "pais": random.choice(["Colombia", "Mexico", "Argentina", "Chile", "Peru"]),
            "ciudad": fake.city(), "fecha_registro": fake.date_time_between(start_date="-2y", end_date="now"),
            "activo": random.choices([True, False], weights=[95, 5])[0],
            "preferencias": {"idioma": random.choice(["es", "en"]), "autoplay": True},
        }
        for i in range(1, NUM_USUARIOS + 1)
    ]
    insertar_en_lotes(db.usuarios, usuarios, "usuarios")
    usuarios_db = list(db.usuarios.find({}, {"_id": 1, "pais": 1}))

    print("🎬 Generando películas...")
    generos = ["Drama", "Comedia", "Accion", "Ciencia ficcion", "Documental", "Suspenso"]
    peliculas = [
        {
            "codigo_pelicula": f"MOV{i:06d}", "titulo": f"{fake.word().capitalize()} {fake.word().capitalize()}",
            "generos": random.sample(generos, random.randint(1, 2)), "anio_estreno": random.randint(1990, ahora.year),
            "duracion_minutos": random.randint(75, 180), "clasificacion": random.choice(["Todo publico", "7+", "13+", "16+", "18+"]),
            "pais_origen": random.choice(["Colombia", "Estados Unidos", "Espana", "Mexico"]),
            "calidad_maxima": random.choice(["HD", "Full HD", "4K"]), "disponible": random.choices([True, False], weights=[90, 10])[0],
            "fecha_publicacion": fake.date_time_between(start_date="-5y", end_date="now"),
        }
        for i in range(1, NUM_PELICULAS + 1)
    ]
    insertar_en_lotes(db.peliculas, peliculas, "peliculas")
    peliculas_db = list(db.peliculas.find({}, {"_id": 1, "titulo": 1, "generos": 1}))

    print("📺 Generando canales de televisión...")
    categorias_canal = ["Noticias", "Deportes", "Infantil", "Musica", "Entretenimiento"]
    canales = [
        {
            "codigo_canal": f"CHN{i:04d}", "nombre": f"Hyper Channel {i:03d}", "categoria": random.choice(categorias_canal),
            "pais": random.choice(["Colombia", "Mexico", "Argentina", "Chile"]), "idioma": random.choice(["es", "en"]),
            "calidad_maxima": random.choice(["HD", "Full HD", "4K"]), "activo": random.choices([True, False], weights=[95, 5])[0],
            "fecha_alta": fake.date_time_between(start_date="-3y", end_date="now"),
        }
        for i in range(1, NUM_CANALES + 1)
    ]
    insertar_en_lotes(db.canales_tv, canales, "canales_tv")
    canales_db = list(db.canales_tv.find({}, {"_id": 1, "nombre": 1, "categoria": 1}))

    print("💳 Generando suscripciones...")
    planes = [("Basico", 14900), ("Estandar", 22900), ("Premium", 29900)]
    suscripciones = []
    for i in range(1, NUM_SUSCRIPCIONES + 1):
        usuario = usuarios_db[(i - 1) % len(usuarios_db)]
        plan, precio = random.choice(planes)
        inicio = fake.date_time_between(start_date="-18m", end_date="now")
        estado = random.choices(["Activa", "Vencida", "Cancelada"], weights=[70, 20, 10])[0]
        suscripciones.append({
            "codigo_suscripcion": f"SUB{i:07d}", "usuario_id": usuario["_id"], "plan": plan, "precio_mensual": precio,
            "fecha_inicio": inicio, "fecha_fin": inicio + timedelta(days=30), "estado": estado, "renovacion_automatica": estado == "Activa",
            "caracteristicas": {"pantallas": {"Basico": 1, "Estandar": 2, "Premium": 4}[plan], "calidad": {"Basico": "HD", "Estandar": "Full HD", "Premium": "4K"}[plan]},
        })
    insertar_en_lotes(db.suscripciones, suscripciones, "suscripciones")
    suscripciones_db = list(db.suscripciones.find({}, {"_id": 1, "usuario_id": 1, "plan": 1, "precio_mensual": 1}))

    print("💰 Generando pagos...")
    pagos = []
    for i in range(1, NUM_PAGOS + 1):
        suscripcion = suscripciones_db[(i - 1) % len(suscripciones_db)]
        pagos.append({
            "codigo_pago": f"PAY{i:08d}", "usuario_id": suscripcion["usuario_id"], "suscripcion_id": suscripcion["_id"],
            "fecha_pago": fake.date_time_between(start_date="-18m", end_date="now"), "monto": suscripcion["precio_mensual"], "moneda": "COP",
            "metodo_pago": random.choice(["Tarjeta de credito", "PSE", "Nequi", "PayPal"]),
            "estado": random.choices(["Completado", "Pendiente", "Rechazado"], weights=[85, 5, 10])[0],
        })
    insertar_en_lotes(db.pagos, pagos, "pagos")

    print("▶️ Generando eventos de reproducción...")
    eventos = []
    contenido = [("pelicula", item) for item in peliculas_db] + [("canal_tv", item) for item in canales_db]
    for i in range(1, NUM_REPRODUCCIONES + 1):
        suscripcion = random.choice(suscripciones_db)
        tipo_contenido, contenido_item = random.choice(contenido)
        fecha_inicio = fake.date_time_between(start_date="-1y", end_date="now")
        duracion = random.randint(2, 150)
        eventos.append({
            "codigo_evento": f"EVT{i:09d}", "usuario_id": suscripcion["usuario_id"], "suscripcion_id": suscripcion["_id"],
            "tipo_contenido": tipo_contenido, "contenido_id": contenido_item["_id"], "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_inicio + timedelta(minutes=duracion), "duracion_vista_minutos": duracion,
            "dispositivo": random.choice(["Smart TV", "Movil", "Web", "Tablet", "Consola"]),
            "pais": random.choice(["Colombia", "Mexico", "Argentina", "Chile", "Peru"]), "calidad": random.choice(["HD", "Full HD", "4K"]),
            "estado": random.choices(["Completada", "Abandonada", "Error"], weights=[75, 20, 5])[0],
        })
        if len(eventos) == BATCH_SIZE:
            insertar_en_lotes(db.reproducciones, eventos, "reproducciones")
            eventos = []
    if eventos:
        insertar_en_lotes(db.reproducciones, eventos, "reproducciones finales")

    print("=" * 60)
    print("🎉 ¡GENERACIÓN DE DATOS HYPERFLIX COMPLETADA!")
    print(f"⏱️ Tiempo total: {time.time() - inicio_total:.2f} segundos")
    for nombre in ["usuarios", "peliculas", "canales_tv", "suscripciones", "pagos", "reproducciones"]:
        print(f"📊 {nombre}: {db[nombre].count_documents({})}")
    print("=" * 60)
    client.close()


if __name__ == "__main__":
    generar_datos()