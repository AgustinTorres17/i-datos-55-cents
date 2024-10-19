import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import unicodedata
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener la conexión a la base de datos
def get_db_connection():
    db_url = os.getenv('DATABASE_URL')
    engine = create_engine(db_url)
    return engine

# Función para normalizar nombres (elimina acentos, convierte a minúsculas, elimina espacios extra)
def normalize_name(name):
    if pd.isnull(name):
        return ''
    # Convertir a minúsculas
    name = name.lower()
    # Eliminar acentos
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')
    # Eliminar espacios extra
    name = ' '.join(name.split())
    return name

# Cargar y preparar los datos de MVP
def load_and_prepare_mvp_csv(csv_path):
    mvp_df = pd.read_csv(csv_path)
    # Verificar los valores únicos en la columna 'MVP'
    print("Valores únicos en la columna 'MVP':", mvp_df['MVP'].unique())
    # Convertir la columna 'MVP' a booleano si es necesario
    if mvp_df['MVP'].dtype != bool:
        mvp_df['MVP'] = mvp_df['MVP'].astype(bool)
    # Normalizar nombres de jugadores
    mvp_df['Player_norm'] = mvp_df['Player'].apply(normalize_name)
    # Filtrar filas donde 'MVP' es True
    mvp_df = mvp_df[mvp_df['MVP'] == True]
    return mvp_df

# Obtener el DataFrame de jugadores desde la base de datos
def get_players_dataframe():
    engine = get_db_connection()
    players_df = pd.read_sql_table('players', con=engine)
    players_df['name_norm'] = players_df['name'].apply(normalize_name)
    return players_df

# Unir los datos de MVP con los jugadores para obtener el idPlayer
def merge_mvp_with_players(mvp_df, players_df):
    merged_df = mvp_df.merge(players_df, left_on='Player_norm', right_on='name_norm', how='left')
    return merged_df

# Manejar jugadores que no se encontraron en la base de datos
def handle_missing_players(merged_df):
    missing_players = merged_df[merged_df['id'].isnull()]['Player'].unique()
    if len(missing_players) > 0:
        print("Jugadores no encontrados en la tabla 'players':")
        for player in missing_players:
            print(f"- {player}")
    # Omitir jugadores sin ID
    merged_df = merged_df[merged_df['id'].notnull()]
    return merged_df

# Preparar los datos para la inserción en la tabla 'mvp'
def prepare_mvp_data(merged_df):
    mvp_data = []
    for _, row in merged_df.iterrows():
        data = {
            'idplayer': int(row['id']),
            'year': row['Season'],
        }
        mvp_data.append(data)
    return mvp_data

# Insertar los datos en la tabla 'mvp'
def insert_mvp_data(mvp_data):
    try:
        engine = get_db_connection()
        Session = sessionmaker(bind=engine)
        session = Session()
        metadata = MetaData()
        mvp_table = Table('mvp', metadata, autoload_with=engine)
        session.execute(mvp_table.insert(), mvp_data)
        session.commit()
        print("Datos de MVP insertados correctamente en 'mvp'.")
    except Exception as e:
        print(f"Error al insertar datos de MVP: {e}")
        session.rollback()
    finally:
        session.close()

# Bloque principal para ejecutar todo el proceso
if __name__ == '__main__':
    # Ruta al CSV de estadísticas de jugadores
    STATS_CSV_PATH = 'data/NBA_Player_Stats.csv'

    # Cargar y preparar los datos de MVP
    mvp_df = load_and_prepare_mvp_csv(STATS_CSV_PATH)
    players_df = get_players_dataframe()
    merged_df = merge_mvp_with_players(mvp_df, players_df)
    merged_df = handle_missing_players(merged_df)
    mvp_data = prepare_mvp_data(merged_df)

    # Insertar en la base de datos
    insert_mvp_data(mvp_data)
