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

# Función para normalizar nombres (elimina acentos, convierte a minúsculas, elimina espacios y puntos)
def normalize_name(name):
    if pd.isnull(name):
        return ''
    name = name.lower()
    name = name.replace('.', '')  # Eliminar puntos
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')
    name = ' '.join(name.split())
    return name.strip()

# Diccionario de mapeo de nombres del CSV a nombres en la base de datos
TEAM_NAME_MAPPING = {
    'atlanta': 'Atlanta Hawks',
    'boston': 'Boston Celtics',
    'brooklyn': 'Brooklyn Nets',
    'new jersey': 'Brooklyn Nets',
    'charlotte': 'Charlotte Hornets',
    'charlotte bobcats': 'Charlotte Hornets',
    'chicago': 'Chicago Bulls',
    'cleveland': 'Cleveland Cavaliers',
    'dallas': 'Dallas Mavericks',
    'denver': 'Denver Nuggets',
    'detroit': 'Detroit Pistons',
    'golden state': 'Golden State Warriors',
    'houston': 'Houston Rockets',
    'indiana': 'Indiana Pacers',
    'la clippers': 'LA Clippers',
    'l a clippers': 'LA Clippers',
    'laclippers': 'LA Clippers',
    'laclippers': 'LA Clippers',
    'los angeles clippers': 'LA Clippers',
    'la lakers': 'Los Angeles Lakers',
    'l a lakers': 'Los Angeles Lakers',
    'lalakers': 'Los Angeles Lakers',
    'los angeles lakers': 'Los Angeles Lakers',
    'memphis': 'Memphis Grizzlies',
    'vancouver': 'Memphis Grizzlies',
    'miami': 'Miami Heat',
    'milwaukee': 'Milwaukee Bucks',
    'minnesota': 'Minnesota Timberwolves',
    'new orleans': 'New Orleans Pelicans',
    'new york': 'New York Knicks',
    'oklahoma city': 'Oklahoma City Thunder',
    'seattle': 'Oklahoma City Thunder',
    'orlando': 'Orlando Magic',
    'philadelphia': 'Philadelphia 76ers',
    'phoenix': 'Phoenix Suns',
    'portland': 'Portland Trail Blazers',
    'sacramento': 'Sacramento Kings',
    'san antonio': 'San Antonio Spurs',
    'toronto': 'Toronto Raptors',
    'utah': 'Utah Jazz',
    'washington': 'Washington Wizards',
}

def map_team_name(name):
    normalized_name = normalize_name(name)
    mapped_name = TEAM_NAME_MAPPING.get(normalized_name)
    if mapped_name:
        # Normalizar el nombre mapeado antes de devolverlo
        return normalize_name(mapped_name)
    else:
        # Devolver el nombre normalizado original si no se encuentra en el mapeo
        return normalized_name



# Leer y preparar el CSV de estadísticas de equipos
def load_and_prepare_team_stats_csv(csv_path):
    # Saltar la segunda fila que contiene encabezados duplicados
    team_stats_df = pd.read_csv(csv_path, skiprows=[1])
    team_stats_df['Team_norm'] = team_stats_df['Team'].apply(map_team_name)
    return team_stats_df

# Obtener el DataFrame de equipos desde la base de datos
def get_teams_dataframe():
    engine = get_db_connection()
    teams_df = pd.read_sql_table('teams', con=engine)
    teams_df['name_norm'] = teams_df['name'].apply(normalize_name)
    return teams_df

def merge_team_stats_with_teams(team_stats_df, teams_df):
    merged_df = team_stats_df.merge(teams_df, left_on='Team_norm', right_on='name_norm', how='left')
    return merged_df

def handle_missing_teams(merged_df):
    missing_teams = merged_df[merged_df['id'].isnull()]['Team'].unique()
    if len(missing_teams) > 0:
        print("Equipos no encontrados en la tabla 'teams' después del merge:")
        for team in missing_teams:
            print(f"- '{team}'")
    else:
        print("Todos los equipos fueron mapeados correctamente.")
    # Omitir equipos sin ID
    merged_df = merged_df[merged_df['id'].notnull()]
    return merged_df


# Procesar y preparar las columnas necesarias del DataFrame
def process_team_stats_columns(df):
    # Renombrar columnas de porcentajes
    df = df.rename(columns={'Pct': 'Fg%', 'Pct.1': '3P%', 'Pct.2': 'Ft%'})

    # Separar 'Fgm-a' en 'Fgm' y 'Fga'
    df[['Fgm', 'Fga']] = df['Fgm-a'].str.split('-', expand=True).astype(float)

    # Separar '3gm-a' en '3pm' y '3pa'
    df[['3pm', '3pa']] = df['3gm-a'].str.split('-', expand=True).astype(float)

    # Separar 'Ftm-a' en 'Ftm' y 'Fta'
    df[['Ftm', 'Fta']] = df['Ftm-a'].str.split('-', expand=True).astype(float)

    # Convertir columnas numéricas adicionales a float
    numeric_cols = ['G', 'Min', 'Pts', 'Reb', 'Ast', 'Stl', 'Blk', 'To', 'Pf',
                    'Dreb', 'Oreb', 'Fg%', '3P%', 'Ft%', 'Eff', 'Deff']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

# Preparar los datos para la inserción en la tabla 'teams_stats'
def prepare_teams_stats_data(merged_df):
    teams_stats_data = []
    for _, row in merged_df.iterrows():
        if row['id'] is None:
            print(f"Equipo sin ID: {row}")
        data = {
            'idteam': int(row['id']),
            'year': row['Year'],
            'games': row['G'],
            'fg': row['Fgm'],
            'fga': row['Fga'],
            'fg_percentage': row['Fg%'],
            'three_points': row['3pm'],
            'three_pa': row['3pa'],
            'three_p_percentage': row['3P%'],
            'ft': row['Ftm'],
            'fta': row['Fta'],
            'ft_percentage': row['Ft%'],
            'orb': row['Oreb'],
            'drb': row['Dreb'],
            'trb': row['Reb'],
            'ast': row['Ast'],
            'stl': row['Stl'],
            'blk': row['Blk'],
            'tov': row['To'],
            'pf': row['Pf'],
            'pts': row['Pts'],
            'eff': row['Eff'],
            'deff': row['Deff'],
        }
        teams_stats_data.append(data)
    return teams_stats_data

# Insertar los datos en la tabla 'teams_stats'
def insert_teams_stats(teams_stats_data):
    try:
        engine = get_db_connection()
        Session = sessionmaker(bind=engine)
        session = Session()
        metadata = MetaData()
        teams_stats_table = Table('teams_stats', metadata, autoload_with=engine)
        session.execute(teams_stats_table.insert(), teams_stats_data)
        session.commit()
        print("Estadísticas de equipos insertadas correctamente en 'teams_stats'.")
    except Exception as e:
        print(f"Error al insertar estadísticas de equipos: {e}")
        session.rollback()
    finally:
        session.close()

# Bloque principal para ejecutar todo el proceso
if __name__ == '__main__':
    # Ruta al CSV de estadísticas de equipos
    TEAM_STATS_CSV_PATH = 'data/NBA_Team_Stats.csv'

    # Cargar y preparar los datos
    team_stats_df = load_and_prepare_team_stats_csv(TEAM_STATS_CSV_PATH)
    team_stats_df = process_team_stats_columns(team_stats_df)
    teams_df = get_teams_dataframe()
    merged_df = merge_team_stats_with_teams(team_stats_df, teams_df)
    merged_df = handle_missing_teams(merged_df)
    teams_stats_data = prepare_teams_stats_data(merged_df)

     # Insertar en la base de datos
    insert_teams_stats(teams_stats_data)
