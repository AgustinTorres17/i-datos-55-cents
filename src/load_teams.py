import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener la conexión a la base de datos
def get_db_connection():
    db_url = os.getenv('DATABASE_URL')
    engine = create_engine(db_url)
    return engine

def insert_team(name, imageurl, abbr, session=None):
    try:
        if session is None:
            engine = get_db_connection()
            # Crear la sesión
            Session = sessionmaker(bind=engine)
            session = Session()
            session_created = True
        else:
            session_created = False

        # Crear los metadatos y acceder a la tabla 'teams'
        metadata = MetaData()
        teams = Table('teams', metadata, autoload_with=session.bind)

        # Crear la inserción
        insert_query = teams.insert().values(name=name, imageurl=imageurl, abbreviation=abbr)

        # Ejecutar la inserción
        session.execute(insert_query)

        if session_created:
            session.commit()
            print(f"Equipo {name} insertado correctamente en la tabla 'teams'.")
    except Exception as e:
        print(f"Error al insertar el equipo {name}: {e}")
        if session_created:
            session.rollback()
    finally:
        if session_created:
            session.close()

def insert_teams(teams_df):
    try:
        engine = get_db_connection()
        # Crear la sesión
        Session = sessionmaker(bind=engine)
        session = Session()

        # Crear los metadatos y acceder a la tabla 'teams'
        metadata = MetaData()
        teams_table = Table('teams', metadata, autoload_with=engine)

        # Preparar una lista de diccionarios con los datos de los equipos
        teams_data = []
        for index, row in teams_df.iterrows():
            id = row['id']
            name = row['name']
            imageurl = row['logo']
            abbr = row['abbreviation']
            teams_data.append({'id': id, 'name': name, 'imageurl': imageurl, 'abreviation': abbr})

        # Ejecutar la inserción en bloque
        session.execute(teams_table.insert(), teams_data)
        session.commit()
        print("Todos los equipos fueron insertados correctamente en la tabla 'teams'.")
    except Exception as e:
        print(f"Error al insertar los equipos: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == '__main__':
    nbaTeams = [
        {"id": 1, "name": "Atlanta Hawks", "logo": "https://upload.wikimedia.org/wikipedia/en/2/24/Atlanta_Hawks_logo.svg", "abbreviation": "ATL"},
        {"id": 2, "name": "Boston Celtics", "logo": "https://upload.wikimedia.org/wikipedia/en/thumb/8/8f/Boston_Celtics.svg/800px-Boston_Celtics.svg.png", "abbreviation": "BOS"},
        {"id": 3, "name": "Brooklyn Nets", "logo": "https://upload.wikimedia.org/wikipedia/commons/thumb/4/44/Brooklyn_Nets_newlogo.svg/800px-Brooklyn_Nets_newlogo.svg.png", "abbreviation": "BKN"},
        {"id": 4, "name": "Charlotte Hornets", "logo": "https://upload.wikimedia.org/wikipedia/en/c/c4/Charlotte_Hornets_%282014%29.svg", "abbreviation": "CHH"},
        {"id": 5, "name": "Chicago Bulls", "logo": "https://upload.wikimedia.org/wikipedia/en/6/67/Chicago_Bulls_logo.svg", "abbreviation": "CHI"},
        {"id": 6, "name": "Cleveland Cavaliers", "logo": "https://seeklogo.com/images/N/nba-cleveland-cavaliers-logo-EC287BF14E-seeklogo.com.png", "abbreviation": "CLE"},
        {"id": 7, "name": "Dallas Mavericks", "logo": "https://upload.wikimedia.org/wikipedia/en/9/97/Dallas_Mavericks_logo.svg", "abbreviation": "DAL"},
        {"id": 8, "name": "Denver Nuggets", "logo": "https://upload.wikimedia.org/wikipedia/en/7/76/Denver_Nuggets.svg", "abbreviation": "DEN"},
        {"id": 9, "name": "Detroit Pistons", "logo": "https://upload.wikimedia.org/wikipedia/commons/3/39/Logo_of_the_Detroit_Pistons.png", "abbreviation": "DET"},
        {"id": 10, "name": "Golden State Warriors", "logo": "https://upload.wikimedia.org/wikipedia/sco/thumb/0/01/Golden_State_Warriors_logo.svg/1200px-Golden_State_Warriors_logo.svg.png", "abbreviation": "GSW"},
        {"id": 11, "name": "Houston Rockets", "logo": "https://upload.wikimedia.org/wikipedia/en/2/28/Houston_Rockets.svg", "abbreviation": "HOU"},
        {"id": 12, "name": "Indiana Pacers", "logo": "https://upload.wikimedia.org/wikipedia/en/1/1b/Indiana_Pacers.svg", "abbreviation": "IND"},
        {"id": 13, "name": "LA Clippers", "logo": "https://upload.wikimedia.org/wikipedia/en/thumb/e/ed/Los_Angeles_Clippers_%282024%29.svg/1200px-Los_Angeles_Clippers_%282024%29.svg.png", "abbreviation": "LAC"},
        {"id": 14, "name": "Los Angeles Lakers", "logo": "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3c/Los_Angeles_Lakers_logo.svg/1200px-Los_Angeles_Lakers_logo.svg.png", "abbreviation": "LAL"},
        {"id": 15, "name": "Memphis Grizzlies", "logo": "https://upload.wikimedia.org/wikipedia/en/f/f1/Memphis_Grizzlies.svg", "abbreviation": "MEM"},
        {"id": 16, "name": "Miami Heat", "logo": "https://upload.wikimedia.org/wikipedia/en/thumb/f/fb/Miami_Heat_logo.svg/1200px-Miami_Heat_logo.svg.png", "abbreviation": "MIA"},
        {"id": 17, "name": "Milwaukee Bucks", "logo": "https://upload.wikimedia.org/wikipedia/en/thumb/4/4a/Milwaukee_Bucks_logo.svg/640px-Milwaukee_Bucks_logo.svg.png", "abbreviation": "MIL"},
        {"id": 18, "name": "Minnesota Timberwolves", "logo": "https://upload.wikimedia.org/wikipedia/en/c/c2/Minnesota_Timberwolves_logo.svg", "abbreviation": "MIN"},
        {"id": 19, "name": "New Orleans Pelicans", "logo": "https://upload.wikimedia.org/wikipedia/en/0/0d/New_Orleans_Pelicans_logo.svg", "abbreviation": "NOP"},
        {"id": 20, "name": "New York Knicks", "logo": "https://upload.wikimedia.org/wikipedia/en/2/25/New_York_Knicks_logo.svg", "abbreviation": "NYK"},
        {"id": 21, "name": "Oklahoma City Thunder", "logo": "https://upload.wikimedia.org/wikipedia/en/5/5d/Oklahoma_City_Thunder.svg", "abbreviation": "OKC"},
        {"id": 22, "name": "Orlando Magic", "logo": "https://upload.wikimedia.org/wikipedia/en/1/10/Orlando_Magic_logo.svg", "abbreviation": "ORL"},
        {"id": 23, "name": "Philadelphia 76ers", "logo": "https://upload.wikimedia.org/wikipedia/commons/e/eb/Philadelphia-76ers-Logo-1977-1996.png", "abbreviation": "PHI"},
        {"id": 24, "name": "Phoenix Suns", "logo": "https://upload.wikimedia.org/wikipedia/en/d/dc/Phoenix_Suns_logo.svg", "abbreviation": "PHX"},
        {"id": 25, "name": "Portland Trail Blazers", "logo": "https://s.yimg.com/cv/apiv2/default/nba/20181221/500x500/trailblazers_wbgs.png", "abbreviation": "POR"},
        {"id": 26, "name": "Sacramento Kings", "logo": "https://upload.wikimedia.org/wikipedia/en/c/c7/SacramentoKings.svg", "abbreviation": "SAC"},
        {"id": 27, "name": "San Antonio Spurs", "logo": "https://upload.wikimedia.org/wikipedia/en/a/a2/San_Antonio_Spurs.svg", "abbreviation": "SAS"},
        {"id": 28, "name": "Toronto Raptors", "logo": "https://upload.wikimedia.org/wikipedia/en/3/36/Toronto_Raptors_logo.svg", "abbreviation": "TOR"},
        {"id": 29, "name": "Utah Jazz", "logo": "https://b.fssta.com/uploads/application/nba/team-logos/Jazz.vresize.350.350.medium.0.png", "abbreviation": "UTA"},
        {"id": 30, "name": "Washington Wizards", "logo": "https://upload.wikimedia.org/wikipedia/en/0/02/Washington_Wizards_logo.svg", "abbreviation": "WAS"}
    ]

    # Convertir los datos a un DataFrame
    teams_df = pd.DataFrame(nbaTeams)

    # Insertar los equipos en la base de datos
    insert_teams(teams_df)
