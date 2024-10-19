from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Crear conexión a la base de datos
def get_db_connection():
    db_url = os.getenv('DATABASE_URL')
    engine = create_engine(db_url)
    print(f"Conexión a la base de datos exitosa: {db_url}")
    return engine

# Crear sesión de base de datos
def get_db_session():
    engine = get_db_connection()
    Session = sessionmaker(bind=engine)
    print("Sesión de base de datos creada.", Session)
    return Session()

get_db_session()