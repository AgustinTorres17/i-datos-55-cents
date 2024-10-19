from db_setup import get_db_session

def test_connection():
    try:
        # Obtener la conexión a la base de datos
        engine = get_db_session()

        # Realizar una consulta simple
        with engine.connect() as connection:
            # Usamos text para pasar la consulta correctamente
            result = connection.execute("SELECT FROM players")
            tables = result.fetchall()

            # Mostrar las tablas disponibles
            if tables:
                print("Tablas en la base de datos:")
                for table in tables:
                    print(table[0])
            else:
                print("No se encontraron tablas en la base de datos.")
        
        print("Conexión exitosa.")
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")

if __name__ == '__main__':
    test_connection()
