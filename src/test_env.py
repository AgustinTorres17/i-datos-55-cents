from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def test_env():
    # Obtener la variable DATABASE_URL
    db_url = os.getenv('DATABASE_URL')

    if db_url:
        print(f"DATABASE_URL cargado correctamente: {db_url}")
    else:
        print("Error: No se encontró la variable DATABASE_URL")

if __name__ == '__main__':
    test_env()
