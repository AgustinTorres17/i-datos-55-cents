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
    name = ' '.join(name.split())  # Eliminar espacios extra
    return name.strip()

# Diccionario de mapeo de nombres del Excel a nombres en la base de datos
TEAM_NAME_MAPPING = {
    'atlanta': 'Atlanta Hawks',
    'st louis hawks': 'Atlanta Hawks',
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
    'fort wayne pistons': 'Detroit Pistons',
    'golden state': 'Golden State Warriors',
    'philadelphia warriors': 'Golden State Warriors',
    'san francisco warriors': 'Golden State Warriors',
    'houston': 'Houston Rockets',
    'indiana': 'Indiana Pacers',
    'la clippers': 'LA Clippers',
    'los angeles clippers': 'LA Clippers',
    'laclippers': 'LA Clippers',
    'la lakers': 'Los Angeles Lakers',
    'los angeles lakers': 'Los Angeles Lakers',
    'lakers': 'Los Angeles Lakers',
    'minneapolis lakers': 'Los Angeles Lakers',
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
    'syracuse nationals': 'Philadelphia 76ers',
    'phoenix': 'Phoenix Suns',
    'portland': 'Portland Trail Blazers',
    'sacramento': 'Sacramento Kings',
    'rochester royals': 'Sacramento Kings',
    'cincinnati royals': 'Sacramento Kings',
    'kansas city kings': 'Sacramento Kings',
    'san antonio': 'San Antonio Spurs',
    'toronto': 'Toronto Raptors',
    'utah': 'Utah Jazz',
    'new orleans jazz': 'Utah Jazz',
    'washington': 'Washington Wizards',
    'baltimore bullets': 'Washington Wizards',
    'washington bullets': 'Washington Wizards',
    'new jersey nets': 'Brooklyn Nets',
    'seattle supersonics': 'Oklahoma City Thunder',
}

# Función para mapear nombres de equipos
def map_team_name(name):
    normalized_name = normalize_name(name)
    mapped_name = TEAM_NAME_MAPPING.get(normalized_name)
    if mapped_name:
        return normalize_name(mapped_name)
    else:
        return normalized_name  # Devolver el nombre normalizado si no se encuentra en el mapeo

# Leer y preparar el Excel de campeones de la NBA
def load_and_prepare_nba_champions_excel(excel_path):
    # Leer el archivo Excel
    df = pd.read_excel(excel_path, engine='openpyxl')

    # Mantener solo las columnas relevantes para los campeones de la NBA
    df = df[['Year', 'NBA Champion']]

    # Normalizar y mapear nombres de equipos
    df['NBA Champion_norm'] = df['NBA Champion'].apply(map_team_name)

    return df

# Obtener el DataFrame de equipos desde la base de datos
def get_teams_dataframe():
    engine = get_db_connection()
    teams_df = pd.read_sql_table('teams', con=engine)
    teams_df['name_norm'] = teams_df['name'].apply(normalize_name)
    return teams_df

# Unir los campeones de la NBA con los equipos para obtener el idTeam
def merge_nba_champions_with_teams(df_champions, teams_df):
    merged_df = df_champions.merge(teams_df, left_on='NBA Champion_norm', right_on='name_norm', how='left')
    return merged_df

# Manejar equipos que no se encontraron en la base de datos
def handle_missing_teams(merged_df):
    missing_teams = merged_df[merged_df['id'].isnull()]['NBA Champion'].unique()
    if len(missing_teams) > 0:
        print("Equipos no encontrados en la tabla 'teams':")
        for team in missing_teams:
            print(f"- '{team}'")
    else:
        print("Todos los equipos fueron mapeados correctamente.")
    # Omitir equipos sin ID
    merged_df = merged_df[merged_df['id'].notnull()]
    return merged_df

# Preparar los datos para la inserción en la tabla 'nba_champions'
def prepare_nba_champions_data(merged_df):
    champions_data = []
    for _, row in merged_df.iterrows():
        data = {
            'idteam': int(row['id']),
            'year': str(row['Year'])
        }
        champions_data.append(data)
    return champions_data

# Insertar los datos en la tabla 'nba_champions'
def insert_nba_champions(champions_data):
    try:
        engine = get_db_connection()
        Session = sessionmaker(bind=engine)
        session = Session()
        metadata = MetaData()
        nba_champions_table = Table('nba_champions', metadata, autoload_with=engine)
        session.execute(nba_champions_table.insert(), champions_data)
        session.commit()
        print("Datos de campeones de la NBA insertados correctamente en 'nba_champions'.")
    except Exception as e:
        print(f"Error al insertar datos de campeones de la NBA: {e}")
        session.rollback()
    finally:
        session.close()

# Bloque principal para ejecutar todo el proceso
if __name__ == '__main__':
    # Ruta al archivo Excel que contiene los datos de campeones de la NBA
    EXCEL_PATH = 'data/NBA Finals and MVP.xlsx'  # Ajusta la ruta según sea necesario

    # Cargar y preparar los datos
    nba_champions_df = load_and_prepare_nba_champions_excel(EXCEL_PATH)
    teams_df = get_teams_dataframe()
    merged_df = merge_nba_champions_with_teams(nba_champions_df, teams_df)
    merged_df = handle_missing_teams(merged_df)
    champions_data = prepare_nba_champions_data(merged_df)

    # Insertar en la base de datos
    insert_nba_champions(champions_data)
