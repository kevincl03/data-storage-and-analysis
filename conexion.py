import os
import pymongo
from dotenv import load_dotenv
from pymongo.errors import ConnectionFailure, ConfigurationError, OperationFailure

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "hyperflix_db")

def conectar_mongodb():
    if not MONGO_URI:
        raise ValueError("Falta la variable de entorno MONGO_URI")
    try:
        print("Intentando conectar a MongoDB Atlas...")
        client = pymongo.MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            appname="hyperflix-data-lake",
        )
        client.admin.command("ping")
        print("Conexión exitosa a MongoDB Atlas")
        
        db = client[MONGO_DB_NAME]
        return client, db
        
    except ConnectionFailure:
        print("❌ Error: No se pudo contactar al servidor.")
    except ConfigurationError as e:
        print(f"❌ Error de configuración: {e}")
    except OperationFailure as e:
        print(f"❌ Error de autenticación: {e}")
    
    return None, None

if __name__ == "__main__":
    client, db = conectar_mongodb()
    if client:
        client.close()
        print("Conexión cerrada.")
