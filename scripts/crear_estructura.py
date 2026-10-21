import os
from datetime import datetime, timedelta

import pymongo
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

def crear_estructura_hyperflix():
    client = pymongo.MongoClient(MONGO_URI)
    db = client["hyperflix_db"]
    
    print("🏗️ Creando estructura de base de datos Hyperflix...\n")
    
    # 1. Crear colecciones OLTP (Transaccionales)
    print("📦 Creando colecciones transaccionales (OLTP)...")
    colecciones = [
        "usuarios", "peliculas", "canales_tv", "suscripciones", "pagos",
        "reproducciones", "reproducciones_analitica",
    ]
    colecciones_existentes = set(db.list_collection_names())
    for nombre_coleccion in colecciones:
        if nombre_coleccion not in colecciones_existentes:
            db.create_collection(nombre_coleccion)
        print(f"  ✅ Colección '{nombre_coleccion}' disponible")
    
    # 2. Crear índices para optimizar consultas
    print("\n🔍 Creando índices...")
    db.usuarios.create_index([("codigo_usuario", 1)], unique=True, name="uk_usuario_codigo")
    db.usuarios.create_index([("email", 1)], unique=True, name="uk_usuario_email")
    db.usuarios.create_index([("activo", 1), ("pais", 1)], name="idx_usuario_estado_pais")
    db.peliculas.create_index([("codigo_pelicula", 1)], unique=True, name="uk_pelicula_codigo")
    db.peliculas.create_index([("generos", 1), ("disponible", 1)], name="idx_pelicula_genero")
    db.canales_tv.create_index([("codigo_canal", 1)], unique=True, name="uk_canal_codigo")
    db.canales_tv.create_index([("categoria", 1), ("activo", 1)], name="idx_canal_categoria")
    db.suscripciones.create_index([("codigo_suscripcion", 1)], unique=True, name="uk_suscripcion_codigo")
    db.suscripciones.create_index([("usuario_id", 1), ("estado", 1)], name="idx_suscripcion_usuario")
    db.pagos.create_index([("codigo_pago", 1)], unique=True, name="uk_pago_codigo")
    db.pagos.create_index([("suscripcion_id", 1), ("estado", 1), ("fecha_pago", -1)], name="idx_pago_suscripcion")
    db.reproducciones.create_index([("codigo_evento", 1)], unique=True, name="uk_reproduccion_codigo")
    db.reproducciones.create_index([("usuario_id", 1), ("fecha_inicio", -1)], name="idx_reproduccion_usuario")
    db.reproducciones.create_index([("tipo_contenido", 1), ("contenido_id", 1)], name="idx_reproduccion_contenido")
    db.reproducciones_analitica.create_index([("anio", 1), ("mes", 1)], name="idx_analitica_fecha")
    print("  ✅ Índices creados")
    
    # 3. Insertar datos de prueba
    print("\n📝 Insertando datos de prueba...")
    
    usuario_ejemplo = {
        "codigo_usuario": "USR000001", "nombre": "Juan Perez",
        "email": "juan.perez@hyperflix.example", "pais": "Colombia",
        "ciudad": "Bogota", "fecha_registro": datetime.now(), "activo": True,
        "preferencias": {"idioma": "es", "clasificacion_maxima": "16+"},
    }
    db.usuarios.update_one({"codigo_usuario": usuario_ejemplo["codigo_usuario"]}, {"$set": usuario_ejemplo}, upsert=True)
    usuario_id = db.usuarios.find_one({"codigo_usuario": usuario_ejemplo["codigo_usuario"]})["_id"]
    print("  ✅ Usuario semilla disponible")
    
    pelicula_ejemplo = {
        "codigo_pelicula": "MOV000001", "titulo": "Horizonte Digital",
        "generos": ["Ciencia ficcion", "Drama"], "anio_estreno": 2025,
        "duracion_minutos": 118, "clasificacion": "16+",
        "pais_origen": "Colombia", "calidad_maxima": "4K", "disponible": True,
        "fecha_publicacion": datetime.now(),
    }
    db.peliculas.update_one({"codigo_pelicula": pelicula_ejemplo["codigo_pelicula"]}, {"$set": pelicula_ejemplo}, upsert=True)
    pelicula_id = db.peliculas.find_one({"codigo_pelicula": pelicula_ejemplo["codigo_pelicula"]})["_id"]
    print("  ✅ Pelicula semilla disponible")
    
    suscripcion_ejemplo = {
        "codigo_suscripcion": "SUB000001", "usuario_id": usuario_id,
        "plan": "Premium", "precio_mensual": 29900,
        "fecha_inicio": datetime.now() - timedelta(days=30),
        "fecha_fin": datetime.now() + timedelta(days=335), "estado": "Activa",
        "renovacion_automatica": True,
        "caracteristicas": ["4K", "4 pantallas", "sin anuncios"],
    }
    db.suscripciones.update_one({"codigo_suscripcion": suscripcion_ejemplo["codigo_suscripcion"]}, {"$set": suscripcion_ejemplo}, upsert=True)
    suscripcion_id = db.suscripciones.find_one({"codigo_suscripcion": suscripcion_ejemplo["codigo_suscripcion"]})["_id"]
    print("  ✅ Suscripcion semilla disponible")
    
    pago_ejemplo = {
        "codigo_pago": "PAY000001", "usuario_id": usuario_id,
        "suscripcion_id": suscripcion_id, "fecha_pago": datetime.now(),
        "monto": 29900, "moneda": "COP", "metodo_pago": "Tarjeta de credito",
        "estado": "Completado",
    }
    db.pagos.update_one({"codigo_pago": pago_ejemplo["codigo_pago"]}, {"$set": pago_ejemplo}, upsert=True)
    print("  ✅ Pago semilla disponible")

    reproduccion_ejemplo = {
        "codigo_evento": "EVT000001", "usuario_id": usuario_id,
        "suscripcion_id": suscripcion_id, "tipo_contenido": "pelicula",
        "contenido_id": pelicula_id, "fecha_inicio": datetime.now(),
        "fecha_fin": datetime.now() + timedelta(minutes=42),
        "duracion_vista_minutos": 42, "dispositivo": "Smart TV",
        "pais": "Colombia", "calidad": "4K", "estado": "Completada",
    }
    db.reproducciones.update_one({"codigo_evento": reproduccion_ejemplo["codigo_evento"]}, {"$set": reproduccion_ejemplo}, upsert=True)
    print("  ✅ Reproduccion semilla disponible")
    
    print("\n✅ Estructura de Hyperflix creada exitosamente en Atlas!")
    print("🔍 Abre MongoDB Compass para ver las colecciones creadas.")
    
    client.close()

if __name__ == "__main__":
    crear_estructura_hyperflix()
