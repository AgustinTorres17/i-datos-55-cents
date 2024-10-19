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

def insert_player(name, position, nba_id, session=None):
    try:
        if session is None:
            engine = get_db_connection()
            # Crear la sesión
            Session = sessionmaker(bind=engine)
            session = Session()
            session_created = True
        else:
            session_created = False

        # Crear los metadatos y acceder a la tabla 'players'
        metadata = MetaData()
        players = Table('players', metadata, autoload_with=session.bind)

        # Crear la inserción
        insert_query = players.insert().values(name=name, position=position, nba_id=nba_id)

        # Ejecutar la inserción
        session.execute(insert_query)

        if session_created:
            session.commit()
            print(f"Jugador {name} insertado correctamente en la tabla 'players'.")
    except Exception as e:
        print(f"Error al insertar el jugador {name}: {e}")
        if session_created:
            session.rollback()
    finally:
        if session_created:
            session.close()


def insert_players(players_df):
    try:
        engine = get_db_connection()
        # Crear la sesión
        Session = sessionmaker(bind=engine)
        session = Session()

        # Crear los metadatos y acceder a la tabla 'players'
        metadata = MetaData()
        players_table = Table('players', metadata, autoload_with=engine)

        # Preparar una lista de diccionarios con los datos de los jugadores
        players_data = []
        for index, row in players_df.iterrows():
            name = row['Player']
            position = row['Pos']
            nba_id = row['NBAID']
            players_data.append({'name': name, 'position': position, 'nba_id': nba_id})

        # Ejecutar la inserción en bloque
        session.execute(players_table.insert(), players_data)
        session.commit()
        print("Todos los jugadores fueron insertados correctamente en la tabla 'players'.")
    except Exception as e:
        print(f"Error al insertar los jugadores: {e}")
        session.rollback()
    finally:
        session.close()



def find_players_with_concatenated_positions(players_csv):
    # Usar 'Player' como identificador único
    player_positions = players_csv.groupby('Player').agg({
        'Pos': lambda x: ', '.join(sorted(set(x)))  # Concatenar las posiciones únicas
    }).reset_index()

    return player_positions


# Función para asignar null a los jugadores con múltiples NBA IDs
def nullify_conflicting_nba_ids(df):
    # Agrupar por 'Player' y contar cuántos NBAID únicos tiene cada jugador
    id_counts = df.groupby('Player')['NBAID'].nunique()

    # Filtrar jugadores con más de 1 NBAID
    conflicting_players = id_counts[id_counts > 1].index

    # Asignar NaN (null) a los registros de jugadores con múltiples IDs
    df.loc[df['Player'].isin(conflicting_players), 'NBAID'] = None

    return df

def clean_player_names(df, column):
    df = df.copy()  # Evitar SettingWithCopyWarning

    # Llenar valores faltantes con cadena vacía
    df[column] = df[column].fillna('')

    # Asegurar que todos los valores sean de tipo string
    df[column] = df[column].astype(str)

    # Eliminar asteriscos y espacios en blanco
    df[column] = df[column].str.replace('*', '', regex=False).str.strip()

    # Eliminar puntos y apóstrofes
    df[column] = df[column].str.replace('.', '', regex=False).str.replace("'", '', regex=False)

    # Convertir a minúsculas
    df[column] = df[column].str.lower()

    # Normalizar caracteres Unicode (eliminar acentos)
    df[column] = df[column].apply(lambda x: unicodedata.normalize('NFKD', x).encode('ASCII', 'ignore').decode('ASCII'))

    return df



def mergeData(players_with_positions, nba_ids):
    # Almacenar el nombre original del jugador
    players_with_positions['OriginalName'] = players_with_positions['Player']
    
    # Limpiar nombres de jugadores en ambos DataFrames
    players_with_positions = clean_player_names(players_with_positions, 'Player')
    nba_ids = clean_player_names(nba_ids, 'NBAName')
    
    # Asegurar que los nombres sean de tipo string
    players_with_positions['Player'] = players_with_positions['Player'].astype(str)
    nba_ids['NBAName'] = nba_ids['NBAName'].astype(str)
    
    # Crear un diccionario para acceso rápido
    nba_ids_dict = nba_ids.set_index('NBAName')['NBAID'].to_dict()
    
    # Lista para almacenar los NBAIDs correspondientes
    nba_id_list = []
    
    # Iterar sobre cada jugador en 'players_with_positions'
    for index, row in players_with_positions.iterrows():
        player_name = row['Player']
        nba_id = nba_ids_dict.get(player_name)
    
        if nba_id is None:
            print(f"No se encontró NBAID para el jugador: {row['Player']}")
    
        nba_id_list.append(nba_id)
    
    # Agregar la lista de NBAIDs al DataFrame 'players_with_positions'
    players_with_positions['NBAID'] = nba_id_list
    
    # Restaurar los nombres originales
    players_with_positions['Player'] = players_with_positions['OriginalName']
    
    # Eliminar la columna 'OriginalName' si ya no es necesaria
    players_with_positions.drop(columns=['OriginalName'], inplace=True)
    
    return players_with_positions



if __name__ == '__main__':
    # Cargar los archivos CSV
    PLAYERS_CSV = pd.read_csv('data/NBA_Player_Stats.csv', delimiter=',')
    NBA_ID_PLAYERS_CSV = pd.read_csv('./data/NBA_Player_IDs.csv', delimiter=',', encoding='ISO-8859-1')

    # Obtener los jugadores con posiciones concatenadas
    players_with_positions = find_players_with_concatenated_positions(PLAYERS_CSV)

    # Obtener los nombres de los jugadores y sus IDs de la NBA
    nba_ids = NBA_ID_PLAYERS_CSV[['NBAName', 'NBAID']]

    print(players_with_positions.head())
    # Unir los DataFrames
    players_with_positions = mergeData(players_with_positions, nba_ids)

    print(players_with_positions.head())
    # Asignar null a los jugadores que tienen más de un NBA ID
    players_with_positions_cleaned = nullify_conflicting_nba_ids(players_with_positions)

    # Imprimir el resultado final
    print(players_with_positions_cleaned[['Player', 'NBAID']].head())

    # Guardar los datos preprocesados (opcional)
    players_with_positions_cleaned.to_csv('data/NBA_Player_Stats_cleaned.csv', index=False)
    

    # Convertir NBAID a enteros, manejando NaN como None
    players_with_positions_cleaned['NBAID'] = players_with_positions_cleaned['NBAID'].apply(lambda x: int(x) if pd.notnull(x) else int(-1))

    # Imprimir el resultado final
    print(players_with_positions_cleaned[['Player', 'NBAID']].head())
    # Guardar los datos preprocesados (opcional)
    players_with_positions_cleaned.to_csv('data/NBA_Player_Stats_cleaned.csv', index=False)

    # Insertar los jugadores en la base de datos
    insert_players(players_with_positions_cleaned)