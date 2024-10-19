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


def get_players_dataframe():
    engine = get_db_connection()
    players_df = pd.read_sql_table('players', con=engine)
    return players_df

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

def load_and_prepare_stats_csv(csv_path):
    stats_df = pd.read_csv(csv_path)
    stats_df['Player_norm'] = stats_df['Player'].apply(normalize_name)
    return stats_df

def merge_stats_with_players(stats_df, players_df):
    players_df['name_norm'] = players_df['name'].apply(normalize_name)
    merged_df = stats_df.merge(players_df, left_on='Player_norm', right_on='name_norm', how='left')
    return merged_df

def handle_missing_players(merged_df):
    missing_players = merged_df[merged_df['id'].isnull()]['Player'].unique()
    if len(missing_players) > 0:
        print("Jugadores no encontrados en la tabla 'players':")
        for player in missing_players:
            print(player)
    # Omitir jugadores sin ID
    merged_df = merged_df[merged_df['id'].notnull()]
    return merged_df


def prepare_players_stats_data(merged_df):
    players_stats_data = []
    for _, row in merged_df.iterrows():
        data = {
            'id_player': int(row['id']),
            'year': row['Season'],
            'team': row['Tm'],
            'games': row['G'],
            'games_started': row['GS'],
            'minutes_played': row['MP'],
            'fg': row['FG'],
            'fga': row['FGA'],
            'fg_percentage': row['FG%'],
            'three_points': row['3P'],
            'three_pa': row['3PA'],
            'three_p_percentage': row['3P%'],
            'two_points': row['2P'],
            'two_pa': row['2PA'],
            'two_p_percentage': row['2P%'],
            'efg_percentage': row['eFG%'],
            'ft': row['FT'],
            'fta': row['FTA'],
            'ft_percentage': row['FT%'],
            'orb': row['ORB'],
            'drb': row['DRB'],
            'trb': row['TRB'],
            'ast': row['AST'],
            'stl': row['STL'],
            'blk': row['BLK'],
            'tov': row['TOV'],
            'pf': row['PF'],
            'pts': row['PTS'],
            'season': row['Season'],
        }
        players_stats_data.append(data)
    return players_stats_data

def insert_players_stats(players_stats_data):
    try:
        engine = get_db_connection()
        Session = sessionmaker(bind=engine)
        session = Session()
        metadata = MetaData()
        players_stats_table = Table('players_stats', metadata, autoload_with=engine)
        session.execute(players_stats_table.insert(), players_stats_data)
        session.commit()
        print("Estadísticas de jugadores insertadas correctamente en 'players_stats'.")
    except Exception as e:
        print(f"Error al insertar estadísticas de jugadores: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == '__main__':
    # Ruta al CSV de estadísticas
    STATS_CSV_PATH = 'data/NBA_Player_Stats.csv'
    
    # Cargar y preparar los datos
    players_stats_df = load_and_prepare_stats_csv(STATS_CSV_PATH)
    players_df = get_players_dataframe()
    merged_df = merge_stats_with_players(players_stats_df, players_df)
    merged_df = handle_missing_players(merged_df)
    merged_df.to_csv('data/NBA_Player_Stats_Out.csv', index=False)
    players_stats_data = prepare_players_stats_data(merged_df)
    
    # Insertar en la base de datos
    insert_players_stats(players_stats_data)

    
