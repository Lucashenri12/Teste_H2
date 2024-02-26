Teste_API_H2

import pandas as pd
import requests
import mysql.connector
from sqlalchemy import create_engine

# Estabelecendo conexão com API
url = "https://odds.p.rapidapi.com/v4/sports/soccer_epl/scores"

headers = {
    'X-RapidAPI-Key': 'd222cb0858msh4522db6049b96d6p1cf6e6jsn3666127d59d5',
    'X-RapidAPI-Host': 'odds.p.rapidapi.com'
}
response = requests.get(url, headers=headers)
data = response.json()

# Estabelescendo conexao local
conf_local = {
   'host': 'localhost',
   'user': 'lucas',
   'password': 'Lucas@2506',
   'database': 'DB_LOCAL',
   'port': '3306'
}

# Criando a tabela 'matchs_epl'
def criar_tabela(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS matchs_epl (
        id INT AUTO_INCREMENT PRIMARY KEY,
        datahora_partida DATETIME,
        data_partida DATE,
        time_casa VARCHAR(100),
        time_fora VARCHAR(100),
        gols_time_casa INT,
        gols_time_fora INT
    )
    """)

# Formatando a estrutura dos dados
data = [
    {'id': '06df5bde57090d9f930d148ba0c7c3fd', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-02-26T20:00:00Z', 'completed': False, 'home_team': 'West Ham United', 'away_team': 'Brentford', 'scores': None, 'last_update': '2024-02-21T19:32:40Z'}, 
    {'id': '946a43585665587575794c942c4837c7', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-02T15:00:00Z', 'completed': False, 'home_team': 'Brentford', 'away_team': 'Chelsea', 'scores': None, 'last_update': None}, 
    {'id': 'dba10d14fc7f9c978eda60d7c8bda3ff', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-02T15:00:00Z', 'completed': False, 'home_team': 'Fulham', 'away_team': 'Brighton and Hove Albion', 'scores': None, 'last_update': None}, 
    {'id': 'ce691e1fa1b3e49cf312a25baf9f777b', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-02T15:00:00Z', 'completed': False, 'home_team': 'Tottenham Hotspur', 'away_team': 'Crystal Palace', 'scores': None, 'last_update': None}, 
    {'id': 'bc98c74b93ace81be110ed2ea62fd309', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-02T15:00:00Z', 'completed': False, 'home_team': 'Everton', 'away_team': 'West Ham United', 'scores': None, 'last_update': None}, 
    {'id': '47a6988aaa5954cd0e0b897fd918a2c0', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-02T15:00:00Z', 'completed': False, 'home_team': 'Nottingham Forest', 'away_team': 'Liverpool', 'scores': None, 'last_update': None}, 
    {'id': '60e491c9ee8fa22e778f5966b0f763ac', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-02T15:00:00Z', 'completed': False, 'home_team': 'Newcastle United', 'away_team': 'Wolverhampton Wanderers', 'scores': None, 'last_update': None}, 
    {'id': '64dbaba16cf215d82da5514f6fadc85e', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-02T17:30:00Z', 'completed': False, 'home_team': 'Luton', 'away_team': 'Aston Villa', 'scores': None, 'last_update': None}, 
    {'id': 'c84fced71c83cd04086763e813bed06c', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-03T13:00:00Z', 'completed': False, 'home_team': 'Burnley', 'away_team': 'Bournemouth', 'scores': None, 'last_update': None}, 
    {'id': 'de86773cb28709b9a7bac7b886b7ab35', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-03T15:30:00Z', 'completed': False, 'home_team': 'Manchester City', 'away_team': 'Manchester United', 'scores': None, 'last_update': None}, 
    {'id': '2c17deae55dde29cfa61cc74c0522504', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-04T20:00:00Z', 'completed': False, 'home_team': 'Sheffield United', 'away_team': 'Arsenal', 'scores': None, 'last_update': None}, 
    {'id': '1bcd8f64a15a2c6518932d591deed057', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-09T12:30:00Z', 'completed': False, 'home_team': 'Manchester United', 'away_team': 'Everton', 'scores': None, 'last_update': None}, 
    {'id': '6479f9fd9078b8403e6c6c991a7b8546', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-09T15:00:00Z', 'completed': False, 'home_team': 'Bournemouth', 'away_team': 'Sheffield United', 'scores': None, 'last_update': None}, 
    {'id': '2de28900073d5b8e71f862cfa2cc7727', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-09T15:00:00Z', 'completed': False, 'home_team': 'Crystal Palace', 'away_team': 'Luton', 'scores': None, 'last_update': None}, 
    {'id': '5de70039c2d4e509d9dbb37eeeb303f5', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-09T15:00:00Z', 'completed': False, 'home_team': 'Wolverhampton Wanderers', 'away_team': 'Fulham', 'scores': None, 'last_update': None}, 
    {'id': 'e815751105aa6ba62f02880fa8825317', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-09T17:30:00Z', 'completed': False, 'home_team': 'Arsenal', 'away_team': 'Brentford', 'scores': None, 'last_update': None}, 
    {'id': 'c75eb09d7f0675e294df8850ca06db85', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-10T13:00:00Z', 'completed': False, 'home_team': 'Aston Villa', 'away_team': 'Tottenham Hotspur', 'scores': None, 'last_update': None}, 
    {'id': '6c8e4ac1f25014b18f47de75c5bf14e6', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-10T14:00:00Z', 'completed': False, 'home_team': 'Brighton and Hove Albion', 'away_team': 'Nottingham Forest', 'scores': None, 'last_update': None}, 
    {'id': 'ab6022886267f39cd1a56a65ed5dc7fd', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-10T14:00:00Z', 'completed': False, 'home_team': 'West Ham United', 'away_team': 'Burnley', 'scores': None, 'last_update': None}, 
    {'id': '828a6e1dd52f38d48ae9c64708dd39af', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-10T15:45:00Z', 'completed': False, 'home_team': 'Liverpool', 'away_team': 'Manchester City', 'scores': None, 'last_update': None}, 
    {'id': '3c614226c781a397f63b53417908fcec', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-11T20:00:00Z', 'completed': False, 'home_team': 'Chelsea', 'away_team': 'Newcastle United', 'scores': None, 'last_update': None}, 
    {'id': 'd756a505b0d891226dc732c7ddd7ae35', 'sport_key': 'soccer_epl', 'sport_title': 'EPL', 'commence_time': '2024-03-13T19:30:00Z', 'completed': False, 'home_team': 'Bournemouth', 'away_team': 'Luton', 'scores': None, 'last_update': None}
]

# Criando um DataFrame a partir dos dados
df = pd.DataFrame(data)

# Criar uma conexão manualmente usando mysql.connector
connection = mysql.connector.connect(
    host=conf_local['host'],
    user=conf_local['user'],
    password=conf_local['password'],
    database=conf_local['database'],
    port=conf_local['port']
)

# Passar a conexão para a engine do SQLAlchemy
engine_local = create_engine('mysql+mysqlconnector://', creator=lambda: connection)

# Inserindo os dados do DF no MySQL
df.to_sql(name='matchs_epl', con=engine_local, if_exists='replace', index=False)